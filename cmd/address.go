package cmd

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/filecoin-project/go-address"
	"github.com/howeyc/gopass"
	cmds "github.com/ipfs/go-ipfs-cmds"
	files "github.com/ipfs/go-ipfs-files"

	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/venus/app/node"
	"github.com/filecoin-project/venus/cmd/tablewriter"
	"github.com/filecoin-project/venus/pkg/crypto"
	"github.com/filecoin-project/venus/pkg/types"
)

var errWalletLocked = errors.New("the wallet is missing password, please use command `venus wallet set-password` to set password")

var walletCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Manage your filecoin wallets",
	},
	Subcommands: map[string]*cmds.Command{
		"balance":      balanceCmd,
		"import":       walletImportCmd,
		// "export":       walletExportCmd,
		"ls":           addrsLsCmd,
		"new":          addrsNewCmd,
		"default":      defaultAddressCmd,
		"set-default":  setDefaultAddressCmd,
		"lock":         lockedCmd,
		"unlock":       unlockedCmd,
		"set-password": setWalletPassword,
	},
}

type AddressResult struct {
	Address address.Address
}

// AddressLsResult is the result of running the address list command.
type AddressLsResult struct {
	Addresses []address.Address
}

var addrsNewCmd = &cmds.Command{
	Options: []cmds.Option{
		cmds.StringOption("type", "The type of address to create: bls (default) or secp256k1").WithDefault("bls"),
	},
	Run: func(req *cmds.Request, re cmds.ResponseEmitter, env cmds.Environment) error {
		protocolName := req.Options["type"].(string)
		var protocol address.Protocol
		switch protocolName {
		case "secp256k1":
			protocol = address.SECP256K1
		case "bls":
			protocol = address.BLS
		default:
			return fmt.Errorf("unrecognized address protocol %s", protocolName)
		}

		if !env.(*node.Env).WalletAPI.HavePassword(req.Context) {
			return errWalletLocked
		}

		addr, err := env.(*node.Env).WalletAPI.WalletNewAddress(protocol)
		if err != nil {
			return err
		}

		return printOneString(re, addr.String())
	},
}

var addrsDelCmd = &cmds.Command{
	Arguments: []cmds.Argument{
		cmds.StringArg("address", true, false, "address to del"),
	},
	Run: func(req *cmds.Request, re cmds.ResponseEmitter, env cmds.Environment) error {
		addr, err := address.NewFromString(req.Arguments[0])
		if err != nil {
			return err
		}

		err = env.(*node.Env).WalletAPI.WalletDelAddress(req.Context, addr)
		if err != nil {
			return err
		}

		return re.Emit(&AddressResult{addr})
	},
	Type: &AddressResult{},
}

var addrsLsCmd = &cmds.Command{
	Options: []cmds.Option{
		cmds.BoolOption("addr-only", "Only print addresses"),
		cmds.BoolOption("id", "Output ID addresses"),
		cmds.BoolOption("market", "Output market balances"),
	},
	Run: func(req *cmds.Request, re cmds.ResponseEmitter, env cmds.Environment) error {
		api := env.(*node.Env)
		ctx := req.Context

		addrs := api.WalletAPI.WalletAddresses()

		// Assume an error means no default key is set
		def, _ := api.WalletAPI.WalletDefaultAddress()

		buf := new(bytes.Buffer)
		tw := tablewriter.New(
			tablewriter.Col("Address"),
			tablewriter.Col("ID"),
			tablewriter.Col("Balance"),
			tablewriter.Col("Market(Avail)"),
			tablewriter.Col("Market(Locked)"),
			tablewriter.Col("Nonce"),
			tablewriter.Col("Default"),
			tablewriter.NewLineCol("Error"))

		addrOnly := false
		if _, ok := req.Options["addr-only"]; ok {
			addrOnly = true
		}
		for _, addr := range addrs {
			if addrOnly {
				writer := NewSilentWriter(buf)
				writer.WriteStringln(addr.String())
			} else {
				a, err := api.ChainAPI.StateGetActor(ctx, addr, types.EmptyTSK)
				if err != nil {
					if !strings.Contains(err.Error(), "actor not found") {
						tw.Write(map[string]interface{}{
							"Address": addr,
							"Error":   err,
						})
						continue
					}

					a = &types.Actor{
						Balance: big.Zero(),
					}
				}

				row := map[string]interface{}{
					"Address": addr,
					"Balance": types.FIL(a.Balance),
					"Nonce":   a.Nonce,
				}
				if addr == def {
					row["Default"] = "X"
				}

				if _, ok := req.Options["id"]; ok {
					id, err := api.ChainAPI.StateLookupID(ctx, addr, types.EmptyTSK)
					if err != nil {
						row["ID"] = "n/a"
					} else {
						row["ID"] = id
					}
				}

				if _, ok := req.Options["market"]; ok {
					mbal, err := api.ChainAPI.StateMarketBalance(ctx, addr, types.EmptyTSK)
					if err == nil {
						row["Market(Avail)"] = types.FIL(crypto.BigSub(mbal.Escrow, mbal.Locked))
						row["Market(Locked)"] = types.FIL(mbal.Locked)
					}
				}

				tw.Write(row)
			}
		}

		if !addrOnly {
			_ = tw.Flush(buf)
		}

		return re.Emit(buf)
	},
}

var defaultAddressCmd = &cmds.Command{
	Run: func(req *cmds.Request, re cmds.ResponseEmitter, env cmds.Environment) error {
		addr, err := env.(*node.Env).WalletAPI.WalletDefaultAddress()
		if err != nil {
			return err
		}

		return printOneString(re, addr.String())
	},
}

var setDefaultAddressCmd = &cmds.Command{
	Arguments: []cmds.Argument{
		cmds.StringArg("address", true, false, "address to set default for"),
	},
	Run: func(req *cmds.Request, re cmds.ResponseEmitter, env cmds.Environment) error {
		addr, err := address.NewFromString(req.Arguments[0])
		if err != nil {
			return err
		}

		err = env.(*node.Env).WalletAPI.WalletSetDefault(context.TODO(), addr)
		if err != nil {
			return err
		}

		return printOneString(re, addr.String())
	},
}

var balanceCmd = &cmds.Command{
	Arguments: []cmds.Argument{
		cmds.StringArg("address", true, false, "APIAddress to get balance for"),
	},
	Run: func(req *cmds.Request, re cmds.ResponseEmitter, env cmds.Environment) error {
		addr, err := address.NewFromString(req.Arguments[0])
		if err != nil {
			return err
		}

		balance, err := env.(*node.Env).WalletAPI.WalletBalance(req.Context, addr)
		if err != nil {
			return err
		}

		return printOneString(re, (types.FIL)(balance).String())
	},
}

// WalletSerializeResult is the type wallet export and import return and expect.
type WalletSerializeResult struct {
	KeyInfo []*crypto.KeyInfo
}

var walletImportCmd = &cmds.Command{
	Arguments: []cmds.Argument{
		cmds.FileArg("walletFile", true, false, "File containing wallet data to import").EnableStdin(),
	},
	Run: func(req *cmds.Request, re cmds.ResponseEmitter, env cmds.Environment) error {
		if !env.(*node.Env).WalletAPI.HavePassword(req.Context) {
			return errWalletLocked
		}
		iter := req.Files.Entries()
		if !iter.Next() {
			return fmt.Errorf("no file given: %s", iter.Err())
		}

		fi, ok := iter.Node().(files.File)
		if !ok {
			return fmt.Errorf("given file was not a files.File")
		}

		var key crypto.KeyInfo
		err := json.NewDecoder(hex.NewDecoder(fi)).Decode(&key)
		if err != nil {
			return err
		}

		addr, err := env.(*node.Env).WalletAPI.WalletImport(&key)
		if err != nil {
			return err
		}

		return printOneString(re, addr.String())
	},
}

var walletExportCmd = &cmds.Command{
	Arguments: []cmds.Argument{
		cmds.StringArg("addr", true, true, "address of key to export"),
		cmds.StringArg("password", false, false, "Password to be locked"),
	},
	PreRun: func(req *cmds.Request, env cmds.Environment) error {
		// for testing, skip manual password entry
		if len(req.Arguments) == 2 && len(req.Arguments[1]) != 0 {
			return nil
		}
		pw, err := gopass.GetPasswdPrompt("Password:", true, os.Stdin, os.Stdout)
		if err != nil {
			return err
		}
		fmt.Println(req.Arguments)
		req.Arguments = []string{req.Arguments[0], string(pw)}

		return nil
	},
	Run: func(req *cmds.Request, re cmds.ResponseEmitter, env cmds.Environment) error {
		if len(req.Arguments) != 2 {
			return re.Emit("Two parameter is required.")
		}
		addr, err := address.NewFromString(req.Arguments[0])
		if err != nil {
			return err
		}

		pw := req.Arguments[1]
		ki, err := env.(*node.Env).WalletAPI.WalletExport(addr, pw)
		if err != nil {
			return err
		}

		kiBytes, err := json.Marshal(ki)
		if err != nil {
			return err
		}

		return printOneString(re, hex.EncodeToString(kiBytes))
	},
}

var setWalletPassword = &cmds.Command{
	Arguments: []cmds.Argument{
		cmds.StringArg("password", false, false, "Password to be locked"),
	},
	PreRun: func(req *cmds.Request, env cmds.Environment) error {
		pw, err := gopass.GetPasswdPrompt("Password:", true, os.Stdin, os.Stdout)
		if err != nil {
			return err
		}
		pw2, err := gopass.GetPasswdPrompt("Enter Password again:", true, os.Stdin, os.Stdout)
		if err != nil {
			return err
		}
		if !bytes.Equal(pw, pw2) {
			return errors.New("the input passwords are inconsistent")
		}

		req.Arguments = []string{string(pw)}

		return nil
	},
	Run: func(req *cmds.Request, re cmds.ResponseEmitter, env cmds.Environment) error {
		if len(req.Arguments) != 1 {
			return re.Emit("A parameter is required.")
		}

		pw := req.Arguments[0]
		if len(pw) == 0 {
			return re.Emit("Do not enter an empty string")
		}

		err := env.(*node.Env).WalletAPI.SetPassword(req.Context, pw)
		if err != nil {
			return err
		}

		return re.Emit("Password set successfully")
	},
}

var lockedCmd = &cmds.Command{
	Arguments: []cmds.Argument{
		cmds.StringArg("password", false, false, "Password to be locked"),
	},
	PreRun: func(req *cmds.Request, env cmds.Environment) error {
		pw, err := gopass.GetPasswdPrompt("Password:", true, os.Stdin, os.Stdout)
		if err != nil {
			return err
		}
		req.Arguments = []string{string(pw)}

		return nil
	},
	Run: func(req *cmds.Request, re cmds.ResponseEmitter, env cmds.Environment) error {
		if len(req.Arguments) != 1 {
			return re.Emit("A parameter is required.")
		}

		pw := req.Arguments[0]

		err := env.(*node.Env).WalletAPI.Locked(req.Context, pw)
		if err != nil {
			return err
		}

		return re.Emit("locked success")
	},
}

var unlockedCmd = &cmds.Command{
	Arguments: []cmds.Argument{
		cmds.StringArg("password", false, false, "Password to be locked"),
	},
	PreRun: func(req *cmds.Request, env cmds.Environment) error {
		pw, err := gopass.GetPasswdPrompt("Password:", true, os.Stdin, os.Stdout)
		if err != nil {
			return err
		}
		req.Arguments = []string{string(pw)}

		return nil
	},
	Run: func(req *cmds.Request, re cmds.ResponseEmitter, env cmds.Environment) error {
		if len(req.Arguments) != 1 {
			return re.Emit("A parameter is required.")
		}

		pw := req.Arguments[0]

		err := env.(*node.Env).WalletAPI.UnLocked(req.Context, pw)
		if err != nil {
			return err
		}

		return re.Emit("unlocked success")
	},
}
