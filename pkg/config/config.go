package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"regexp"
	"strings"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/pkg/errors"

	"github.com/filecoin-project/venus/pkg/constants"
)

const (
	scryptN = 1 << 15
	scryptP = 1
)

// Config is an in memory representation of the filecoin configuration file
type Config struct {
	API           *APIConfig           `json:"api"`
	Bootstrap     *BootstrapConfig     `json:"bootstrap"`
	Datastore     *DatastoreConfig     `json:"datastore"`
	Mpool         *MessagePoolConfig   `json:"mpool"`
	NetworkParams *NetworkParamsConfig `json:"parameters"`
	Observability *ObservabilityConfig `json:"observability"`
	Swarm         *SwarmConfig         `json:"swarm"`
	Wallet        *WalletConfig        `json:"walletModule"`
}

// APIConfig holds all configuration options related to the api.
type APIConfig struct {
	APIAddress                    string   `json:"apiAddress"`
	AccessControlAllowOrigin      []string `json:"accessControlAllowOrigin"`
	AccessControlAllowCredentials bool     `json:"accessControlAllowCredentials"`
	AccessControlAllowMethods     []string `json:"accessControlAllowMethods"`
}

func newDefaultAPIConfig() *APIConfig {
	return &APIConfig{
		APIAddress: "/ip4/0.0.0.0/tcp/3453",
		AccessControlAllowOrigin: []string{
			"http://localhost:8080",
			"https://localhost:8080",
			"http://127.0.0.1:8080",
			"https://127.0.0.1:8080",
		},
		AccessControlAllowMethods: []string{"GET", "POST", "PUT"},
	}
}

// DatastoreConfig holds all the configuration options for the datastore.
// TODO: use the advanced datastore configuration from ipfs
type DatastoreConfig struct {
	Type string `json:"type"`
	Path string `json:"path"`
}

// Validators hold the list of validation functions for each configuration
// property. Validators must take a key and json string respectively as
// arguments, and must return either an error or nil depending on whether or not
// the given key and value are valid. Validators will only be run if a property
// being set matches the name given in this map.
var Validators = map[string]func(string, string) error{
	"heartbeat.nickname": validateLettersOnly,
}

func newDefaultDatastoreConfig() *DatastoreConfig {
	return &DatastoreConfig{
		Type: "badgerds",
		Path: "badger",
	}
}

// SwarmConfig holds all configuration options related to the swarm.
type SwarmConfig struct {
	Address            string `json:"address"`
	PublicRelayAddress string `json:"public_relay_address,omitempty"`
}

func newDefaultSwarmConfig() *SwarmConfig {
	return &SwarmConfig{
		Address: "/ip4/0.0.0.0/tcp/6000",
	}
}

// BootstrapConfig holds all configuration options related to bootstrap nodes
type BootstrapConfig struct {
	Addresses        []string `json:"addresses"`
	MinPeerThreshold int      `json:"minPeerThreshold"`
	Period           string   `json:"period,omitempty"`
}

// TODO: provide bootstrap node addresses
func newDefaultBootstrapConfig() *BootstrapConfig {
	return &BootstrapConfig{
		Addresses:        []string{},
		MinPeerThreshold: 3, // TODO: we don't actually have an bootstrap peers yet.
		Period:           "1m",
	}
}

// WalletConfig holds all configuration options related to the wallet.
type WalletConfig struct {
	DefaultAddress   address.Address  `json:"defaultAddress,omitempty"`
	PassphraseConfig PassphraseConfig `json:"passphraseConfig,omitempty"`
}

type PassphraseConfig struct {
	ScryptN int `json:"scryptN"`
	ScryptP int `json:"scryptP"`
}

func newDefaultWalletConfig() *WalletConfig {
	return &WalletConfig{
		DefaultAddress:   address.Undef,
		PassphraseConfig: DefaultPassphraseConfig(),
	}
}

func DefaultPassphraseConfig() PassphraseConfig {
	return PassphraseConfig{
		ScryptN: scryptN,
		ScryptP: scryptP,
	}
}

// DrandConfig holds all configuration options related to pulling randomness from Drand servers
type DrandConfig struct {
	StartTimeUnix int64 `json:"startTimeUnix"`
	RoundSeconds  int   `json:"roundSeconds"`
}

// HeartbeatConfig holds all configuration options related to node heartbeat.
type HeartbeatConfig struct {
	// BeatTarget represents the address the filecoin node will send heartbeats to.
	BeatTarget string `json:"beatTarget"`
	// BeatPeriod represents how frequently heartbeats are sent.
	// Golang duration units are accepted.
	BeatPeriod string `json:"beatPeriod"`
	// ReconnectPeriod represents how long the node waits before attempting to reconnect.
	// Golang duration units are accepted.
	ReconnectPeriod string `json:"reconnectPeriod"`
	// Nickname represents the nickname of the filecoin node,
	Nickname string `json:"nickname"`
}

// ObservabilityConfig is a container for configuration related to observables.
type ObservabilityConfig struct {
	Metrics *MetricsConfig `json:"metrics"`
	Tracing *TraceConfig   `json:"tracing"`
}

func newDefaultObservabilityConfig() *ObservabilityConfig {
	return &ObservabilityConfig{
		Metrics: newDefaultMetricsConfig(),
		Tracing: newDefaultTraceConfig(),
	}
}

// MetricsConfig holds all configuration options related to node metrics.
type MetricsConfig struct {
	// Enabled will enable prometheus metrics when true.
	PrometheusEnabled bool `json:"prometheusEnabled"`
	// ReportInterval represents how frequently filecoin will update its prometheus metrics.
	ReportInterval string `json:"reportInterval"`
	// PrometheusEndpoint represents the address filecoin will expose prometheus metrics at.
	PrometheusEndpoint string `json:"prometheusEndpoint"`
}

func newDefaultMetricsConfig() *MetricsConfig {
	return &MetricsConfig{
		PrometheusEnabled:  false,
		ReportInterval:     "5s",
		PrometheusEndpoint: "/ip4/0.0.0.0/tcp/9400",
	}
}

// TraceConfig holds all configuration options related to enabling and exporting
// filecoin node traces.
type TraceConfig struct {
	// JaegerTracingEnabled will enable exporting traces to jaeger when true.
	JaegerTracingEnabled bool `json:"jaegerTracingEnabled"`
	// ProbabilitySampler will sample fraction of traces, 1.0 will sample all traces.
	ProbabilitySampler float64 `json:"probabilitySampler"`
	// JaegerEndpoint is the URL traces are collected on.
	JaegerEndpoint string `json:"jaegerEndpoint"`
}

func newDefaultTraceConfig() *TraceConfig {
	return &TraceConfig{
		JaegerEndpoint:       "http://localhost:14268/api/traces",
		JaegerTracingEnabled: false,
		ProbabilitySampler:   1.0,
	}
}

// MessagePoolConfig holds all configuration options related to nodes message pool (mpool).
type MessagePoolConfig struct {
	// MaxPoolSize is the maximum number of pending messages will will allow in the message pool at any time
	MaxPoolSize uint `json:"maxPoolSize"`
	// MaxNonceGap is the maximum nonce of a message past the last received on chain
	MaxNonceGap uint64 `json:"maxNonceGap"`
}

func newDefaultMessagePoolConfig() *MessagePoolConfig {
	return &MessagePoolConfig{
		MaxPoolSize: 1000000,
		MaxNonceGap: 100,
	}
}

type NetworkParamsConfig struct {
	ConsensusMinerMinPower uint64                       `json:"consensusMinerMinPower"` // uint64 goes up to 18 EiB
	MinVerifiedDealSize    int64                        `json:"minVerifiedDealSize"`
	ReplaceProofTypes      []int64                      `json:"replaceProofTypes"`
	BlockDelay             uint64                       `json:"blockDelay"`
	DrandSchedule          map[abi.ChainEpoch]DrandEnum `json:"drandSchedule"`
	ForkUpgradeParam       *ForkUpgradeConfig           `json:"forkUpgradeParam"`
	AddressNetwork         address.Network              `json:"addressNetwork"`
}

type ForkUpgradeConfig struct {
	UpgradeSmokeHeight       abi.ChainEpoch `json:"upgradeSmokeHeight"`
	UpgradeBreezeHeight      abi.ChainEpoch `json:"upgradeBreezeHeight"`
	UpgradeIgnitionHeight    abi.ChainEpoch `json:"upgradeIgnitionHeight"`
	UpgradeLiftoffHeight     abi.ChainEpoch `json:"upgradeLiftoffHeight"`
	UpgradeActorsV2Height    abi.ChainEpoch `json:"upgradeActorsV2Height"`
	UpgradeRefuelHeight      abi.ChainEpoch `json:"upgradeRefuelHeight"`
	UpgradeTapeHeight        abi.ChainEpoch `json:"upgradeTapeHeight"`
	UpgradeKumquatHeight     abi.ChainEpoch `json:"upgradeKumquatHeight"`
	BreezeGasTampingDuration abi.ChainEpoch `json:"breezeGasTampingDuration"`
	UpgradeCalicoHeight      abi.ChainEpoch `json:"upgradeCalicoHeight"`
	UpgradePersianHeight     abi.ChainEpoch `json:"upgradePersianHeight"`
	UpgradeOrangeHeight      abi.ChainEpoch `json:"upgradeOrangeHeight"`
	UpgradeClausHeight       abi.ChainEpoch `json:"upgradeClausHeight"`
	UpgradeActorsV3Height    abi.ChainEpoch `json:"upgradeActorsV3Height"`
}

func IsNearUpgrade(epoch, upgradeEpoch abi.ChainEpoch) bool {
	return epoch > upgradeEpoch-constants.Finality && epoch < upgradeEpoch+constants.Finality
}

var DefaultForkUpgradeParam = &ForkUpgradeConfig{
	UpgradeBreezeHeight:      41280,
	BreezeGasTampingDuration: 120,
	UpgradeSmokeHeight:       51000,
	UpgradeIgnitionHeight:    94000,
	UpgradeRefuelHeight:      130800,
	UpgradeTapeHeight:        140760,
	UpgradeLiftoffHeight:     148888,
	UpgradeKumquatHeight:     170000,
	UpgradeCalicoHeight:      265200,
	UpgradePersianHeight:     265200 + 120*60,
	UpgradeActorsV2Height:    138720,
	UpgradeOrangeHeight:      336458,
	UpgradeClausHeight:       343200,
	UpgradeActorsV3Height:    -1,
}

func newDefaultNetworkParamsConfig() *NetworkParamsConfig {
	defaultParams := *DefaultForkUpgradeParam
	return &NetworkParamsConfig{
		ConsensusMinerMinPower: 0, // 0 means don't override the value
		ReplaceProofTypes: []int64{
			int64(abi.RegisteredSealProof_StackedDrg2KiBV1),
			int64(abi.RegisteredSealProof_StackedDrg512MiBV1),
			int64(abi.RegisteredSealProof_StackedDrg32GiBV1),
			int64(abi.RegisteredSealProof_StackedDrg64GiBV1),
		},
		DrandSchedule:    map[abi.ChainEpoch]DrandEnum{0: 5, -1: 1},
		ForkUpgradeParam: &defaultParams,
	}
}

// NewDefaultConfig returns a config object with all the fields filled out to
// their default values
func NewDefaultConfig() *Config {
	return &Config{
		API:           newDefaultAPIConfig(),
		Bootstrap:     newDefaultBootstrapConfig(),
		Datastore:     newDefaultDatastoreConfig(),
		Mpool:         newDefaultMessagePoolConfig(),
		NetworkParams: newDefaultNetworkParamsConfig(),
		Observability: newDefaultObservabilityConfig(),
		Swarm:         newDefaultSwarmConfig(),
		Wallet:        newDefaultWalletConfig(),
	}
}

// WriteFile writes the config to the given filepath.
func (cfg *Config) WriteFile(file string) error {
	f, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close() // nolint: errcheck

	configString, err := json.MarshalIndent(*cfg, "", "\t")
	if err != nil {
		return err
	}

	_, err = fmt.Fprint(f, string(configString))
	return err
}

// ReadFile reads a config file from disk.
func ReadFile(file string) (*Config, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}

	cfg := NewDefaultConfig()
	rawConfig, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}
	if len(rawConfig) == 0 {
		return cfg, nil
	}

	err = json.Unmarshal(rawConfig, &cfg)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

// Set sets the config sub-struct referenced by `key`, e.g. 'api.address'
// or 'datastore' to the json key value pair encoded in jsonVal.
func (cfg *Config) Set(dottedKey string, jsonString string) error {
	if !json.Valid([]byte(jsonString)) {
		jsonBytes, _ := json.Marshal(jsonString)
		jsonString = string(jsonBytes)
	}

	if err := validate(dottedKey, jsonString); err != nil {
		return err
	}

	keys := strings.Split(dottedKey, ".")
	for i := len(keys) - 1; i >= 0; i-- {
		jsonString = fmt.Sprintf(`{ "%s": %s }`, keys[i], jsonString)
	}

	decoder := json.NewDecoder(strings.NewReader(jsonString))
	decoder.DisallowUnknownFields()

	return decoder.Decode(&cfg)
}

// Get gets the config sub-struct referenced by `key`, e.g. 'api.address'
func (cfg *Config) Get(key string) (interface{}, error) {
	v := reflect.Indirect(reflect.ValueOf(cfg))
	keyTags := strings.Split(key, ".")
OUTER:
	for j, keyTag := range keyTags {
		if v.Type().Kind() == reflect.Struct {
			for i := 0; i < v.NumField(); i++ {
				jsonTag := strings.Split(
					v.Type().Field(i).Tag.Get("json"),
					",")[0]
				if jsonTag == keyTag {
					v = v.Field(i)
					if j == len(keyTags)-1 {
						return v.Interface(), nil
					}
					v = reflect.Indirect(v) // only attempt one dereference
					continue OUTER
				}
			}
		}

		return nil, fmt.Errorf("key: %s invalid for config", key)
	}
	// Cannot get here as len(strings.Split(s, sep)) >= 1 with non-empty sep
	return nil, fmt.Errorf("empty key is invalid")
}

// validate runs validations on a given key and json string. validate uses the
// validators map defined at the top of this file to determine which validations
// to use for each key.
func validate(dottedKey string, jsonString string) error {
	var obj interface{}
	if err := json.Unmarshal([]byte(jsonString), &obj); err != nil {
		return err
	}
	// recursively validate sub-keys by partially unmarshalling
	if reflect.ValueOf(obj).Kind() == reflect.Map {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal([]byte(jsonString), &obj); err != nil {
			return err
		}
		for key := range obj {
			if err := validate(dottedKey+"."+key, string(obj[key])); err != nil {
				return err
			}
		}
		return nil
	}

	if validationFunc, present := Validators[dottedKey]; present {
		return validationFunc(dottedKey, jsonString)
	}

	return nil
}

// validateLettersOnly validates that a given value contains only letters. If it
// does not, an error is returned using the given key for the message.
func validateLettersOnly(key string, value string) error {
	if match, _ := regexp.MatchString("^\"[a-zA-Z]+\"$", value); !match {
		return errors.Errorf(`"%s" must only contain letters`, key)
	}
	return nil
}
