package network

import (
	"context"

	"github.com/filecoin-project/venus/pkg/constants"
	"github.com/filecoin-project/venus/pkg/net"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/metrics"
	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

type NetworkAPI struct { //nolint
	network *NetworkSubmodule
}

// NetworkGetBandwidthStats gets stats on the current bandwidth usage of the network
func (networkAPI *NetworkAPI) NetworkGetBandwidthStats() metrics.Stats {
	return networkAPI.network.Network.GetBandwidthStats()
}

// NetworkGetPeerAddresses gets the current addresses of the node
func (networkAPI *NetworkAPI) NetworkGetPeerAddresses() []ma.Multiaddr {
	return networkAPI.network.Network.GetPeerAddresses()
}

// NetworkGetPeerID gets the current peer id of the node
func (networkAPI *NetworkAPI) NetworkGetPeerID() peer.ID {
	return networkAPI.network.Network.GetPeerID()
}

// NetworkFindProvidersAsync issues a findProviders query to the filecoin network content router.
func (networkAPI *NetworkAPI) NetworkFindProvidersAsync(ctx context.Context, key cid.Cid, count int) <-chan peer.AddrInfo {
	return networkAPI.network.Network.Router.FindProvidersAsync(ctx, key, count)
}

// NetworkGetClosestPeers issues a getClosestPeers query to the filecoin network.
func (networkAPI *NetworkAPI) NetworkGetClosestPeers(ctx context.Context, key string) (<-chan peer.ID, error) {
	return networkAPI.network.Network.GetClosestPeers(ctx, key)
}

// NetworkFindPeer searches the libp2p router for a given peer id
func (networkAPI *NetworkAPI) NetworkFindPeer(ctx context.Context, peerID peer.ID) (peer.AddrInfo, error) {
	return networkAPI.network.Network.FindPeer(ctx, peerID)
}

// NetworkConnect connects to peers at the given addresses
func (networkAPI *NetworkAPI) NetworkConnect(ctx context.Context, addrs []string) (<-chan net.ConnectionResult, error) {
	return networkAPI.network.Network.Connect(ctx, addrs)
}

// NetworkPeers lists peers currently available on the network
func (networkAPI *NetworkAPI) NetworkPeers(ctx context.Context, verbose, latency, streams bool) (*net.SwarmConnInfos, error) {
	return networkAPI.network.Network.Peers(ctx, verbose, latency, streams)
}

// Version provides various build-time information
type Version struct {
	Version string

	// APIVersion is a binary encoded semver version of the remote implementing
	// this api
	//
	// See APIVersion in build/version.go
	APIVersion constants.Version
}

func (networkAPI *NetworkAPI) Version(context.Context) (Version, error) {
	return Version{
		Version:    constants.UserVersion(),
		APIVersion: constants.FullAPIVersion,
	}, nil
}

func (networkAPI *NetworkAPI) NetAddrsListen(context.Context) (peer.AddrInfo, error) {
	return peer.AddrInfo{
		ID:    networkAPI.network.Host.ID(),
		Addrs: networkAPI.network.Host.Addrs(),
	}, nil
}
