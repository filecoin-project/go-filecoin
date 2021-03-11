package networks

import (
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/venus/pkg/constants"

	"github.com/filecoin-project/venus/pkg/config"
)

func Net2k() *NetworkConf {
	return &NetworkConf{
		Bootstrap: config.BootstrapConfig{
			Addresses:        []string{},
			MinPeerThreshold: 0,
			Period:           "30s",
		},
		Network: config.NetworkParamsConfig{
			NetworkType:            constants.Network2k,
			BlockDelay:             4,
			ConsensusMinerMinPower: 2048,
			MinVerifiedDealSize:    256,
			ReplaceProofTypes: []int64{
				int64(abi.RegisteredSealProof_StackedDrg2KiBV1),
			},
			ForkUpgradeParam: &config.ForkUpgradeConfig{
				UpgradeBreezeHeight:   -1,
				UpgradeSmokeHeight:    -1,
				UpgradeIgnitionHeight: -2,
				UpgradeRefuelHeight:   -3,
				UpgradeActorsV2Height: 10,
				UpgradeTapeHeight:     -4,
				UpgradeLiftoffHeight:  -5,
				UpgradeKumquatHeight:  15,
				UpgradeCalicoHeight:   20,
				UpgradePersianHeight:  25,
				UpgradeOrangeHeight:   888888888888,
				UpgradeActorsV3Height: -1,

				BreezeGasTampingDuration: 0,
				UpgradeClausHeight:       30,
			},
			DrandSchedule:  map[abi.ChainEpoch]config.DrandEnum{0: 1},
			AddressNetwork: address.Testnet,
		},
	}
}
