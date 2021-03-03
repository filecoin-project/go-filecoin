package constants

import (
	"fmt"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/venus/build/flags"
)

var BuildType int

const (
	BuildDefault  = 0
	BuildMainnet  = 0x1
	Build2k       = 0x2
	BuildDebug    = 0x3
	BuildCalibnet = 0x4
)

func SetBuildType(network string) error {
	switch network {
	case "mainnet":
		BuildType = BuildMainnet
	case "testnetnet":
		BuildType = BuildMainnet
	case "integrationnet":
		BuildType |= BuildDebug
	case "2k":
		BuildType |= Build2k
	case "cali":
		BuildType = BuildCalibnet
	default:
		return fmt.Errorf("unknown network name %s", network)
	}

	return nil
}

// BuildVersion is the local build version, set by build system
const BuildVersion = "1.2.2"

func UserVersion() string {
	return BuildVersion + flags.GitCommit
}

type Version uint32

func newVer(major, minor, patch uint8) Version {
	return Version(uint32(major)<<16 | uint32(minor)<<8 | uint32(patch))
}

type NodeType int

const (
	NodeUnknown NodeType = iota

	NodeFull
	NodeMiner
	NodeWorker
)

var RunningNodeType NodeType

func VersionForType(nodeType NodeType) (Version, error) {
	switch nodeType {
	case NodeFull:
		return FullAPIVersion, nil
	case NodeMiner:
		return MinerAPIVersion, nil
	case NodeWorker:
		return WorkerAPIVersion, nil
	default:
		return Version(0), xerrors.Errorf("unknown node type %d", nodeType)
	}
}

// semver versions of the rpc api exposed
var (
	FullAPIVersion   = newVer(1, 1, 0)
	MinerAPIVersion  = newVer(1, 0, 1)
	WorkerAPIVersion = newVer(1, 0, 0)
)
