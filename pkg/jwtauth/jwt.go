package jwtauth

import (
	"context"
	"fmt"
	"strings"

	"github.com/filecoin-project/go-jsonrpc/auth"
	jwt3 "github.com/gbrlsnchs/jwt/v3"
	logging "github.com/ipfs/go-log"
	acrypto "github.com/libp2p/go-libp2p-core/crypto"
	xerrors "github.com/pkg/errors"

	"github.com/filecoin-project/venus/pkg/repo"
)

type APIAlg jwt3.HMACSHA

var jwtLog = logging.Logger("jwt")

var (
	ErrKeyInfoNotFound = fmt.Errorf("key info not found")
)

type JwtPayload struct {
	Allow []auth.Permission
}

type JwtAuth struct {
	apiSecret     *APIAlg
	jwtSecetName  string
	jwtHmacSecret string
	payload       JwtPayload
	lr            repo.Repo
}

type JwtAuthAPI struct { //nolint
	JwtAuth *JwtAuth
}

func NewJwtAuth(lr repo.Repo) (*JwtAuth, error) {
	jwtAuth := &JwtAuth{
		jwtSecetName:  "auth-jwt-private",
		jwtHmacSecret: "jwt-hmac-secret",
		lr:            lr,
		payload:       JwtPayload{Allow: []auth.Permission{"all"}},
	}
	var err error
	jwtAuth.apiSecret, err = jwtAuth.loadAPISecret()
	if err != nil {
		return nil, err
	}
	return jwtAuth, nil
}

func (jwtAuth *JwtAuth) loadAPISecret() (*APIAlg, error) {
	pk, err := jwtAuth.lr.Keystore().Get(jwtAuth.jwtHmacSecret)
	//todo use custome keystore to replace
	if err != nil && strings.Contains(err.Error(), "no key by the given name was found") {
		jwtLog.Warn("Generating new API secret")

		pk, _, err = acrypto.GenerateKeyPair(acrypto.RSA, 2048)
		if err != nil {
			return nil, xerrors.Wrap(err, "failed to create peer key")
		}
		if err := jwtAuth.lr.Keystore().Put(jwtAuth.jwtHmacSecret, pk); err != nil {
			return nil, xerrors.Wrap(err, "failed to store private key")
		}
		raw, _ := pk.Raw()
		cliToken, err := jwt3.Sign(&jwtAuth.payload, jwt3.NewHS256(raw))
		if err != nil {
			return nil, err
		}

		if err := jwtAuth.lr.SetAPIToken(cliToken); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, xerrors.Errorf("could not get JWT Token: %v", err)
	}
	raw, _ := pk.Raw()
	return (*APIAlg)(jwt3.NewHS256(raw)), nil
}

func (jwtAuth *JwtAuth) API() *JwtAuthAPI {
	return &JwtAuthAPI{JwtAuth: jwtAuth}
}

func (a *JwtAuthAPI) AuthVerify(ctx context.Context, token string) ([]auth.Permission, error) {
	//var payload JwtPayload
	//if _, err := jwt3.Verify([]byte(token), (*jwt3.HMACSHA)(a.JwtAuth.apiSecret), &payload); err != nil {
	//	return nil, xerrors.Errorf("JWT Verification failed: %v", err)
	//}
	//
	//return payload.Allow, nil

	return []auth.Permission{"all"}, nil
}

func (a *JwtAuthAPI) AuthNew(ctx context.Context, perms []auth.Permission) ([]byte, error) {
	p := JwtPayload{
		Allow: perms, // TODO: consider checking validity
	}

	return jwt3.Sign(&p, (*jwt3.HMACSHA)(a.JwtAuth.apiSecret))
}
