package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"

	"golang.org/x/xerrors"

	_ "net/http/pprof" // nolint: golint

	blockstore "github.com/ipfs/go-ipfs-blockstore"
	cmds "github.com/ipfs/go-ipfs-cmds"
	cbor "github.com/ipfs/go-ipld-cbor"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car"
	"github.com/libp2p/go-libp2p-core/crypto"

	"github.com/filecoin-project/venus/app/node"
	"github.com/filecoin-project/venus/app/paths"
	"github.com/filecoin-project/venus/fixtures/asset"
	"github.com/filecoin-project/venus/fixtures/networks"
	"github.com/filecoin-project/venus/pkg/config"
	"github.com/filecoin-project/venus/pkg/constants"
	"github.com/filecoin-project/venus/pkg/genesis"
	"github.com/filecoin-project/venus/pkg/journal"
	"github.com/filecoin-project/venus/pkg/repo"
	"github.com/filecoin-project/venus/pkg/types"
	gengen "github.com/filecoin-project/venus/tools/gengen/util"
)

var log = logging.Logger("daemon")

const (
	makeGenFlag     = "make-genesis"
	preTemplateFlag = "genesis-template"
)

var daemonCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Initialize a venus repo, Start a long-running daemon process",
	},
	Options: []cmds.Option{
		cmds.StringOption(makeGenFlag, "make genesis"),
		cmds.StringOption(preTemplateFlag, "template for make genesis"),
		cmds.StringOption(SwarmAddress, "multiaddress to listen on for filecoin network connections"),
		cmds.StringOption(SwarmPublicRelayAddress, "public multiaddress for routing circuit relay traffic.  Necessary for relay nodes to provide this if they are not publically dialable"),
		cmds.BoolOption(OfflineMode, "start the node without networking"),
		cmds.BoolOption(ELStdout),
		cmds.BoolOption(IsRelay, "advertise and allow venus network traffic to be relayed through this node"),
		cmds.StringOption(ImportSnapshot, "import chain state from a given chain export file or url"),
		cmds.StringOption(GenesisFile, "path of file or HTTP(S) URL containing archive of genesis block DAG data"),
		cmds.StringOption(PeerKeyFile, "path of file containing key to use for new node's libp2p identity"),
		cmds.StringOption(WalletKeyFile, "path of file containing keys to import into the wallet on initialization"),
		cmds.StringOption(Network, "when set, populates config with network specific parameters").WithDefault("testnetnet"),
	},
	Run: func(req *cmds.Request, re cmds.ResponseEmitter, env cmds.Environment) error {
		network, _ := req.Options[Network].(string)
		if err := constants.SetBuildType(network); err != nil {
			log.Errorf("Error setting BuildType %s", err)
			return err
		}

		repoDir, _ := req.Options[OptionRepoDir].(string)
		repoDir, err := paths.GetRepoPath(repoDir)
		if err != nil {
			return err
		}

		exist, err := repo.Exists(repoDir)
		if err != nil {
			return err
		}
		if !exist {
			log.Infof("Initializing repo at '%s'", repoDir)

			if err := re.Emit(repoDir); err != nil {
				return err
			}
			if err := repo.InitFSRepo(repoDir, repo.Version, config.NewDefaultConfig()); err != nil {
				return err
			}

			if err = initRun(req); err != nil {
				return err
			}
		}

		return daemonRun(req, re)
	},
}

func initRun(req *cmds.Request) error {
	rep, err := getRepo(req)
	if err != nil {
		return err
	}

	// The only error Close can return is that the repo has already been closed.
	defer func() {
		_ = rep.Close()
	}()

	var genesisFunc genesis.InitFunc
	cfg := rep.Config()
	network, _ := req.Options[Network].(string)
	if err := setConfigFromOptions(cfg, network); err != nil {
		log.Errorf("Error setting config %s", err)
		return err
	}

	// genesis node
	node.SetNetParams(cfg.NetworkParams)
	if mkGen, ok := req.Options[makeGenFlag].(string); ok {
		preTp := req.Options[preTemplateFlag]
		if preTp == nil {
			return xerrors.Errorf("must also pass file with genesis template to `--%s`", preTemplateFlag)
		}

		genesisFunc = genesis.MakeGenesis(req.Context, rep, mkGen, preTp.(string), cfg.NetworkParams.ForkUpgradeParam)
	} else {
		genesisFileSource, _ := req.Options[GenesisFile].(string)
		genesisFunc, err = loadGenesis(req.Context, rep, genesisFileSource, network)
		if err != nil {
			return err
		}
	}

	peerKeyFile, _ := req.Options[PeerKeyFile].(string)
	walletKeyFile, _ := req.Options[WalletKeyFile].(string)
	initOpts, err := getNodeInitOpts(peerKeyFile, walletKeyFile)
	if err != nil {
		return err
	}

	if err := rep.ReplaceConfig(cfg); err != nil {
		log.Errorf("Error replacing config %s", err)
		return err
	}

	if err := node.Init(req.Context, rep, genesisFunc, initOpts...); err != nil {
		log.Errorf("Error initializing node %s", err)
		return err
	}

	return nil
}

func daemonRun(req *cmds.Request, re cmds.ResponseEmitter) error {
	// third precedence is config file.
	rep, err := getRepo(req)
	if err != nil {
		return err
	}

	config := rep.Config()

	// second highest precedence is env vars.
	if envAPI := os.Getenv("FIL_API"); envAPI != "" {
		config.API.APIAddress = envAPI
	}

	// highest precedence is cmd line flag.
	if flagAPI, ok := req.Options[OptionAPI].(string); ok && flagAPI != "" {
		config.API.APIAddress = flagAPI
	}

	if swarmAddress, ok := req.Options[SwarmAddress].(string); ok && swarmAddress != "" {
		config.Swarm.Address = swarmAddress
	}

	if publicRelayAddress, ok := req.Options[SwarmPublicRelayAddress].(string); ok && publicRelayAddress != "" {
		config.Swarm.PublicRelayAddress = publicRelayAddress
	}

	opts, err := node.OptionsFromRepo(rep)
	if err != nil {
		return err
	}

	if offlineMode, ok := req.Options[OfflineMode].(bool); ok {
		opts = append(opts, node.OfflineMode(offlineMode))
	}

	if isRelay, ok := req.Options[IsRelay].(bool); ok && isRelay {
		opts = append(opts, node.IsRelay())
	}
	importPath, _ := req.Options[ImportSnapshot].(string)
	if len(importPath) != 0 {
		err := Import(rep, importPath)
		if err != nil {
			log.Errorf("failed to import snapshot, import path: %s, error: %s", importPath, err.Error())
			return err
		}
	}

	journal, err := journal.NewZapJournal(rep.JournalPath())
	if err != nil {
		return err
	}
	opts = append(opts, node.JournalConfigOption(journal))

	// Monkey-patch network parameters option will set package variables during node build
	opts = append(opts, node.MonkeyPatchNetworkParamsOption(config.NetworkParams))

	// Instantiate the node.
	fcn, err := node.New(req.Context, opts...)
	if err != nil {
		return err
	}

	if fcn.OfflineMode() {
		_ = re.Emit("Filecoin node running in offline mode (libp2p is disabled)\n")
	} else {
		_ = re.Emit(fmt.Sprintf("My peer ID is %s\n", fcn.Network().Host.ID().Pretty()))
		for _, a := range fcn.Network().Host.Addrs() {
			_ = re.Emit(fmt.Sprintf("Swarm listening on: %s\n", a))
		}
	}

	if _, ok := req.Options[ELStdout].(bool); ok {
		_ = re.Emit("--" + ELStdout + " option is deprecated\n")
	}

	// Start the node.
	if err := fcn.Start(req.Context); err != nil {
		return err
	}
	defer fcn.Stop(req.Context)

	// Run API server around the node.
	ready := make(chan interface{}, 1)
	go func() {
		<-ready
		lines := []string{
			fmt.Sprintf("API server listening on %s\n", config.API.APIAddress),
		}
		_ = re.Emit(lines)
	}()

	// The request is expected to remain open so the daemon uses the request context.
	// Pass a new context here if the flow changes such that the command should exit while leaving
	// a forked deamon running.
	return fcn.RunRPCAndWait(req.Context, RootCmdDaemon, ready)
}

func getRepo(req *cmds.Request) (repo.Repo, error) {
	repoDir, _ := req.Options[OptionRepoDir].(string)
	repoDir, err := paths.GetRepoPath(repoDir)
	if err != nil {
		return nil, err
	}
	return repo.OpenFSRepo(repoDir, repo.Version)
}

func setConfigFromOptions(cfg *config.Config, network string) error {
	// Setup specific config options.
	var netcfg *networks.NetworkConf
	switch network {
	case "mainnet":
		netcfg = networks.Mainnet()
	case "testnetnet":
		netcfg = networks.Testnet()
	case "integrationnet":
		netcfg = networks.IntegrationNet()
	case "2k":
		netcfg = networks.Net2k()
	case "cali":
		netcfg = networks.Calibration()
	default:
		return fmt.Errorf("unknown network name %s", network)
	}

	if netcfg != nil {
		cfg.Bootstrap = &netcfg.Bootstrap
		cfg.NetworkParams = &netcfg.Network
	}

	return nil
}

func loadGenesis(ctx context.Context, rep repo.Repo, sourceName string, network string) (genesis.InitFunc, error) {
	var (
		source io.ReadCloser
		err    error
	)

	if sourceName == "" {
		var bs []byte
		switch network {
		case "cali":
			bs, err = asset.Asset("fixtures/_assets/car/calibnet.car")
			if err != nil {
				return gengen.MakeGenesisFunc(), nil
			}
		default:
			bs, err = asset.Asset("fixtures/_assets/car/devnet.car")
			if err != nil {
				return gengen.MakeGenesisFunc(), nil
			}
		}

		source = ioutil.NopCloser(bytes.NewReader(bs))
		// return gengen.MakeGenesisFunc(), nil
	} else {
		source, err = openGenesisSource(sourceName)
		if err != nil {
			return nil, err
		}
	}

	defer func() { _ = source.Close() }()

	genesisBlk, err := extractGenesisBlock(source, rep)
	if err != nil {
		return nil, err
	}

	gif := func(cst cbor.IpldStore, bs blockstore.Blockstore) (*types.BlockHeader, error) {
		return genesisBlk, err
	}

	return gif, nil

}

func getNodeInitOpts(peerKeyFile string, walletKeyFile string) ([]node.InitOpt, error) {
	var initOpts []node.InitOpt
	if peerKeyFile != "" {
		data, err := ioutil.ReadFile(peerKeyFile)
		if err != nil {
			return nil, err
		}
		peerKey, err := crypto.UnmarshalPrivateKey(data)
		if err != nil {
			return nil, err
		}
		initOpts = append(initOpts, node.PeerKeyOpt(peerKey))
	}

	if walletKeyFile != "" {
		f, err := os.Open(walletKeyFile)
		if err != nil {
			return nil, err
		}

		var wir *WalletSerializeResult
		if err := json.NewDecoder(f).Decode(&wir); err != nil {
			return nil, err
		}

		if len(wir.KeyInfo) > 0 {
			initOpts = append(initOpts, node.DefaultKeyOpt(wir.KeyInfo[0]))
		}

		for _, k := range wir.KeyInfo[1:] {
			initOpts = append(initOpts, node.ImportKeyOpt(k))
		}
	}

	return initOpts, nil
}

func openGenesisSource(sourceName string) (io.ReadCloser, error) {
	sourceURL, err := url.Parse(sourceName)
	if err != nil {
		return nil, fmt.Errorf("invalid filepath or URL for genesis file: %s", sourceURL)
	}
	var source io.ReadCloser
	if sourceURL.Scheme == "http" || sourceURL.Scheme == "https" {
		// NOTE: This code is temporary. It allows downloading a genesis block via HTTP(S) to be able to join a
		// recently deployed staging devnet.
		response, err := http.Get(sourceName)
		if err != nil {
			return nil, err
		}
		source = response.Body
	} else if sourceURL.Scheme != "" {
		return nil, fmt.Errorf("unsupported protocol for genesis file: %s", sourceURL.Scheme)
	} else {
		file, err := os.Open(sourceName)
		if err != nil {
			return nil, err
		}
		source = file
	}
	return source, nil
}

func extractGenesisBlock(source io.ReadCloser, rep repo.Repo) (*types.BlockHeader, error) {
	bs := rep.Datastore()
	ch, err := car.LoadCar(bs, source)
	if err != nil {
		return nil, err
	}

	// need to check if we are being handed a car file with a single genesis block or an entire chain.
	bsBlk, err := bs.Get(ch.Roots[0])
	if err != nil {
		return nil, err
	}
	cur, err := types.DecodeBlock(bsBlk.RawData())
	if err != nil {
		return nil, err
	}

	return cur, nil
}
