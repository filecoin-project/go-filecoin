package networks

import (
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/venus/pkg/config"
	"github.com/filecoin-project/venus/pkg/constants"
)

func Calibration() *NetworkConf {
	return &NetworkConf{
		Bootstrap: config.BootstrapConfig{
			Addresses: []string{
				"/dns4/bootstrap-0.calibration.fildev.network/tcp/1347/p2p/12D3KooWRLZAseMo9h7fRD6ojn6YYDXHsBSavX5YmjBZ9ngtAEec",
				"/dns4/bootstrap-1.calibration.fildev.network/tcp/1347/p2p/12D3KooWJFtDXgZEQMEkjJPSrbfdvh2xfjVKrXeNFG1t8ioJXAzv",
				"/dns4/bootstrap-2.calibration.fildev.network/tcp/1347/p2p/12D3KooWP1uB9Lo7yCA3S17TD4Y5wStP5Nk7Vqh53m8GsFjkyujD",
				"/dns4/bootstrap-3.calibration.fildev.network/tcp/1347/p2p/12D3KooWLrPM4WPK1YRGPCUwndWcDX8GCYgms3DiuofUmxwvhMCn",
			},
			MinPeerThreshold: 0,
			Period:           "30s",
		},
		Network: config.NetworkParamsConfig{
			NetworkType:            constants.NetworkCalibnet,
			BlockDelay:             30,
			ConsensusMinerMinPower: 32 << 30,
			ReplaceProofTypes: []int64{
				int64(abi.RegisteredSealProof_StackedDrg32GiBV1),
				int64(abi.RegisteredSealProof_StackedDrg64GiBV1),
			},
			ForkUpgradeParam: &config.ForkUpgradeConfig{
				UpgradeBreezeHeight:   -1,
				UpgradeSmokeHeight:    -2,
				UpgradeIgnitionHeight: -3,
				UpgradeRefuelHeight:   -4,
				UpgradeActorsV2Height: 30,
				UpgradeTapeHeight:     60,
				UpgradeLiftoffHeight:  -5,
				UpgradeKumquatHeight:  90,
				UpgradeCalicoHeight:   100,
				UpgradePersianHeight:  100 + (120 * 1),
				UpgradeOrangeHeight:   300,
				UpgradeActorsV3Height: 600,

				BreezeGasTampingDuration: 120,
				UpgradeClausHeight:       250,
			},
			DrandSchedule:  map[abi.ChainEpoch]config.DrandEnum{0: 1},
			AddressNetwork: address.Testnet,
		},
	}
}
