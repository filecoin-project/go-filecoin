package messagepool

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/filecoin-project/go-state-types/big"
	"github.com/ipfs/go-datastore"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/venus/pkg/repo"
)

var (
	ReplaceByFeeRatioDefault  = 1.25
	MemPoolSizeLimitHiDefault = 30000
	MemPoolSizeLimitLoDefault = 20000
	PruneCooldownDefault      = time.Minute
	GasLimitOverestimation    = 1.25

	ConfigKey    = datastore.NewKey("/mpool/config")
	ConfigGasKey = datastore.NewKey("/mpool/gas/config")
)

type MpoolConfig struct {
	PriorityAddrs          []address.Address
	SizeLimitHigh          int
	SizeLimitLow           int
	ReplaceByFeeRatio      float64
	PruneCooldown          time.Duration
	GasLimitOverestimation float64
}

func (mc *MpoolConfig) Clone() *MpoolConfig {
	r := new(MpoolConfig)
	*r = *mc
	return r
}

func loadConfig(ds repo.Datastore) (*MpoolConfig, error) {
	haveCfg, err := ds.Has(ConfigKey)
	if err != nil {
		return nil, err
	}

	if !haveCfg {
		return DefaultConfig(), nil
	}

	cfgBytes, err := ds.Get(ConfigKey)
	if err != nil {
		return nil, err
	}
	cfg := new(MpoolConfig)
	err = json.Unmarshal(cfgBytes, cfg)
	return cfg, err
}

func saveConfig(cfg *MpoolConfig, ds repo.Datastore) error {
	cfgBytes, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	return ds.Put(ConfigKey, cfgBytes)
}

func (mp *MessagePool) GetConfig() *MpoolConfig {
	mp.cfgLk.Lock()
	defer mp.cfgLk.Unlock()
	return mp.cfg.Clone()
}

func validateConfg(cfg *MpoolConfig) error {
	if cfg.ReplaceByFeeRatio < ReplaceByFeeRatioDefault {
		return fmt.Errorf("'ReplaceByFeeRatio' is less than required %f < %f",
			cfg.ReplaceByFeeRatio, ReplaceByFeeRatioDefault)
	}
	if cfg.GasLimitOverestimation < 1 {
		return fmt.Errorf("'GasLimitOverestimation' cannot be less than 1")
	}
	return nil
}

func (mp *MessagePool) SetConfig(cfg *MpoolConfig) error {
	if err := validateConfg(cfg); err != nil {
		return err
	}
	cfg = cfg.Clone()

	mp.cfgLk.Lock()
	mp.cfg = cfg
	err := saveConfig(cfg, mp.ds)
	if err != nil {
		log.Warnf("error persisting mpool config: %s", err)
	}
	mp.cfgLk.Unlock()

	return nil
}

func DefaultConfig() *MpoolConfig {
	return &MpoolConfig{
		SizeLimitHigh:          MemPoolSizeLimitHiDefault,
		SizeLimitLow:           MemPoolSizeLimitLoDefault,
		ReplaceByFeeRatio:      ReplaceByFeeRatioDefault,
		PruneCooldown:          PruneCooldownDefault,
		GasLimitOverestimation: GasLimitOverestimation,
	}
}

type MpoolGasConfig struct {
	GasPremium big.Int
	GasFeeGap  big.Int
}

func (mgc *MpoolGasConfig) Clone() *MpoolGasConfig {
	c := new(MpoolGasConfig)
	*c = *mgc
	return c
}

func loadGasConfig(ds repo.Datastore) (*MpoolGasConfig, error) {
	haveCfg, err := ds.Has(ConfigGasKey)
	if err != nil {
		return nil, err
	}

	if !haveCfg {
		return DefaultGasConfig(), nil
	}

	gCfgBytes, err := ds.Get(ConfigGasKey)
	if err != nil {
		return nil, err
	}
	gCfg := new(MpoolGasConfig)
	err = json.Unmarshal(gCfgBytes, gCfg)
	return gCfg, err
}

func saveGasConfig(gCfg *MpoolGasConfig, ds repo.Datastore) error {
	cfgBytes, err := json.Marshal(gCfg)
	if err != nil {
		return err
	}
	return ds.Put(ConfigGasKey, cfgBytes)
}

func (mp *MessagePool) GetGasConfig() *MpoolGasConfig {
	mp.gcfgLk.Lock()
	defer mp.gcfgLk.Unlock()
	return mp.gcfg.Clone()
}

func (mp *MessagePool) SetGasConfig(gcfg *MpoolGasConfig) error {
	gcfg = gcfg.Clone()

	mp.gcfgLk.Lock()
	mp.gcfg = gcfg
	err := saveGasConfig(gcfg, mp.ds)
	if err != nil {
		log.Warnf("error persisting mpool gas config: %s", err)
	}
	mp.gcfgLk.Unlock()

	return nil
}

func DefaultGasConfig() *MpoolGasConfig {
	return &MpoolGasConfig{
		GasPremium: big.NewInt(0),
		GasFeeGap:  big.NewInt(0),
	}
}
