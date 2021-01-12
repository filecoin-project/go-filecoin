package wallet

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync"

	"github.com/filecoin-project/go-address"
	logging "github.com/ipfs/go-log/v2"
	"github.com/pkg/errors"

	"github.com/filecoin-project/venus/pkg/crypto"
)

const TestPassword = "test-password"

var ErrKeyInfoNotFound = fmt.Errorf("key info not found")
var walletLog = logging.Logger("wallet")

// wallet manages the locally stored addresses.
type Wallet struct {
	lk sync.Mutex

	backends map[reflect.Type][]Backend
}

// New constructs a new wallet, that manages addresses in all the
// passed in backends.
func New(backends ...Backend) *Wallet {
	backendsMap := make(map[reflect.Type][]Backend)

	for _, backend := range backends {
		kind := reflect.TypeOf(backend)
		backendsMap[kind] = append(backendsMap[kind], backend)
	}

	return &Wallet{
		backends: backendsMap,
	}
}

// HasAddress checks if the given address is stored.
// Safe for concurrent access.
func (w *Wallet) HasAddress(a address.Address) bool {
	_, err := w.Find(a)
	return err == nil
}

// Find searches through all backends and returns the one storing the passed
// in address.
// Safe for concurrent access.
func (w *Wallet) Find(addr address.Address) (Backend, error) {
	w.lk.Lock()
	defer w.lk.Unlock()

	for _, backends := range w.backends {
		for _, backend := range backends {
			if backend.HasAddress(addr) {
				return backend, nil
			}
		}
	}

	return nil, fmt.Errorf("wallet has no address %s", addr)
}

// Addresses retrieves all stored addresses.
// Safe for concurrent access.
// Always sorted in the same order.
func (w *Wallet) Addresses() []address.Address {
	w.lk.Lock()
	defer w.lk.Unlock()

	var out []address.Address
	for _, backends := range w.backends {
		for _, backend := range backends {
			out = append(out, backend.Addresses()...)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i].Bytes(), out[j].Bytes()) < 0
	})

	return out
}

// Backends returns backends by their kind.
func (w *Wallet) Backends(kind reflect.Type) []Backend {
	w.lk.Lock()
	defer w.lk.Unlock()

	cpy := make([]Backend, len(w.backends[kind]))
	copy(cpy, w.backends[kind])
	return cpy
}

// SignBytes cryptographically signs `data` using the private key corresponding to
// address `addr`
func (w *Wallet) SignBytes(data []byte, addr address.Address) (*crypto.Signature, error) {
	// Check that we are storing the address to sign for.
	backend, err := w.Find(addr)
	if err != nil {
		return nil, errors.Wrapf(err, "could not find address: %s", addr)
	}
	return backend.SignBytes(data, addr)
}

// NewAddress creates a new account address on the default wallet backend.
func (w *Wallet) NewAddress(p address.Protocol) (address.Address, error) {
	backend, err := w.DSBacked()
	if err != nil {
		return address.Undef, err
	}
	return backend.NewAddress(p)
}

// GetPubKeyForAddress returns the public key in the keystore associated with
// the given address.
func (w *Wallet) GetPubKeyForAddress(addr address.Address) ([]byte, error) {
	info, err := w.keyInfoForAddr(addr)
	if err != nil {
		return nil, err
	}

	return info.PublicKey(), nil
}

// NewKeyInfo creates a new KeyInfo struct in the wallet backend and returns it
func (w *Wallet) NewKeyInfo() (*crypto.KeyInfo, error) {
	newAddr, err := w.NewAddress(address.BLS)
	if err != nil {
		return &crypto.KeyInfo{}, err
	}

	return w.keyInfoForAddr(newAddr)
}

func (w *Wallet) keyInfoForAddr(addr address.Address) (*crypto.KeyInfo, error) {
	backend, err := w.Find(addr)
	if err != nil {
		return &crypto.KeyInfo{}, err
	}

	info, err := backend.GetKeyInfo(addr)
	if err != nil {
		return &crypto.KeyInfo{}, err
	}
	return info, nil
}

// Import adds the given keyinfo to the wallet
func (w *Wallet) Import(ki *crypto.KeyInfo) (address.Address, error) {
	dsb := w.Backends(DSBackendType)
	if len(dsb) != 1 {
		return address.Undef, fmt.Errorf("expected exactly one datastore wallet backend")
	}

	imp, ok := dsb[0].(Importer)
	if !ok {
		return address.Undef, fmt.Errorf("datastore backend wallets should implement importer")
	}

	if err := imp.ImportKey(ki); err != nil {
		return address.Undef, err
	}

	a, err := ki.Address()
	if err != nil {
		return address.Undef, err
	}
	return a, nil
}

// Export returns the KeyInfos for the given wallet addresses
func (w *Wallet) Export(addr address.Address, password string) (*crypto.KeyInfo, error) {
	bck, err := w.Find(addr)
	if err != nil {
		return nil, err
	}

	ki, err := bck.GetKeyInfoPassphrase(addr, keccak256([]byte(password)))
	if err != nil {
		return nil, err
	}

	return ki, nil
}

func (w *Wallet) WalletSign(ctx context.Context, addr address.Address, msg []byte, meta MsgMeta) (*crypto.Signature, error) {
	ki, err := w.Find(addr)
	if err != nil {
		return nil, err
	}
	if ki == nil {
		return nil, errors.Errorf("signing using key '%s': %v", addr.String(), ErrKeyInfoNotFound)
	}

	return ki.SignBytes(msg, addr)
}

func (w *Wallet) DSBacked() (*DSBackend, error) {
	backends := w.Backends(DSBackendType)
	if len(backends) == 0 {
		return nil, errors.Errorf("missing default ds backend")
	}

	return (backends[0]).(*DSBackend), nil
}

func (w *Wallet) Locked(password string) error {
	backend, err := w.DSBacked()
	if err != nil {
		return err
	}

	return backend.Locked(password)
}

func (w *Wallet) UnLocked(password string) error {
	backend, err := w.DSBacked()
	if err != nil {
		return err
	}
	return backend.UnLocked(password)
}

func (w *Wallet) SetPassword(password string) error {
	backend, err := w.DSBacked()
	if err != nil {
		return err
	}
	return backend.SetPassword(password)
}

func (w *Wallet) HavePassword() bool {
	backend, err := w.DSBacked()
	if err != nil {
		walletLog.Errorf("get DSBacked failed: %v", err)
		return false
	}
	return backend.HavePassword()
}

func (w *Wallet) WalletDelete(ctx context.Context, addr address.Address) error {
	bk, err := w.Find(addr)
	if err != nil {
		return err
	}

	backends := w.Backends(DSBackendType)
	if len(backends) == 0 {
		return fmt.Errorf("missing default ds backend")
	}

	backend := (backends[0]).(*DSBackend)

	ki, err := bk.GetKeyInfo(addr)
	if err != nil {
		return err
	}

	return backend.DeleteKey(ki)
}
