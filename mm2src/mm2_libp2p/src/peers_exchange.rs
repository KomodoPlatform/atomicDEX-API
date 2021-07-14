use crate::request_response::Codec;
use crate::NetworkInfo;
use futures::StreamExt;
use libp2p::swarm::NetworkBehaviour;
use libp2p::{multiaddr::{Multiaddr, Protocol},
             request_response::{handler::RequestProtocol, ProtocolName, ProtocolSupport, RequestResponse,
                                RequestResponseConfig, RequestResponseEvent, RequestResponseMessage},
             swarm::{NetworkBehaviourAction, NetworkBehaviourEventProcess, PollParameters},
             NetworkBehaviour, PeerId};
use log::{error, info, warn};
use rand::seq::SliceRandom;
use serde::{de::Deserializer, ser::Serializer, Deserialize, Serialize};
use std::collections::HashSet;
use std::{collections::{HashMap, VecDeque},
          iter,
          task::{Context, Poll},
          time::Duration};
use wasm_timer::{Instant, Interval};

pub type PeerAddresses = HashSet<Multiaddr>;

#[derive(Debug, Clone)]
pub enum PeersExchangeProtocol {
    Version1,
}

impl ProtocolName for PeersExchangeProtocol {
    fn protocol_name(&self) -> &[u8] {
        match self {
            PeersExchangeProtocol::Version1 => b"/peers-exchange/1",
        }
    }
}

type PeersExchangeCodec = Codec<PeersExchangeProtocol, PeersExchangeRequest, PeersExchangeResponse>;

const DEFAULT_PEERS_NUM: usize = 20;
const REQUEST_PEERS_INITIAL_DELAY: u64 = 20;
const REQUEST_PEERS_INTERVAL: u64 = 300;
const MAX_PEERS: usize = 100;

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct PeerIdSerde(PeerId);

impl From<PeerId> for PeerIdSerde {
    fn from(peer_id: PeerId) -> PeerIdSerde { PeerIdSerde(peer_id) }
}

impl Serialize for PeerIdSerde {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.clone().to_bytes().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PeerIdSerde {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes: Vec<u8> = Deserialize::deserialize(deserializer)?;
        let peer_id = PeerId::from_bytes(&bytes).map_err(|_| serde::de::Error::custom("PeerId::from_bytes error"))?;
        Ok(PeerIdSerde(peer_id))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum PeersExchangeRequest {
    GetKnownPeers { num: usize },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum PeersExchangeResponse {
    KnownPeers { peers: HashMap<PeerIdSerde, PeerAddresses> },
}

/// Behaviour that requests known peers list from other peers at random
#[derive(NetworkBehaviour)]
#[behaviour(poll_method = "poll")]
pub struct PeersExchange {
    request_response: RequestResponse<PeersExchangeCodec>,
    #[behaviour(ignore)]
    known_peers: Vec<PeerId>,
    #[behaviour(ignore)]
    events: VecDeque<NetworkBehaviourAction<RequestProtocol<PeersExchangeCodec>, ()>>,
    #[behaviour(ignore)]
    maintain_peers_interval: Interval,
    #[behaviour(ignore)]
    network_info: NetworkInfo,
}

#[allow(clippy::new_without_default)]
impl PeersExchange {
    pub fn new(network_info: NetworkInfo) -> Self {
        let codec = Codec::default();
        let protocol = iter::once((PeersExchangeProtocol::Version1, ProtocolSupport::Full));
        let config = RequestResponseConfig::default();
        let request_response = RequestResponse::new(codec, protocol, config);
        PeersExchange {
            request_response,
            known_peers: Vec::new(),
            events: VecDeque::new(),
            maintain_peers_interval: Interval::new_at(
                Instant::now() + Duration::from_secs(REQUEST_PEERS_INITIAL_DELAY),
                Duration::from_secs(REQUEST_PEERS_INTERVAL),
            ),
            network_info,
        }
    }

    fn get_random_known_peers(&mut self, num: usize) -> HashMap<PeerIdSerde, PeerAddresses> {
        let mut result = HashMap::with_capacity(num);
        let mut rng = rand::thread_rng();
        let peer_ids = self.known_peers.choose_multiple(&mut rng, num).cloned();
        for peer_id in peer_ids {
            let addresses = self.request_response.addresses_of_peer(&peer_id).into_iter().collect();
            result.insert(peer_id.into(), addresses);
        }
        result
    }

    fn forget_peer(&mut self, peer: &PeerId) {
        self.known_peers.retain(|known_peer| known_peer != peer);
        self.forget_peer_addresses(peer);
    }

    fn forget_peer_addresses(&mut self, peer: &PeerId) {
        for address in self.request_response.addresses_of_peer(&peer) {
            self.request_response.remove_address(&peer, &address);
        }
    }

    pub fn add_peer_addresses(&mut self, peer: &PeerId, addresses: PeerAddresses) {
        if addresses.len() > 1 {
            return;
        }

        for address in addresses.iter() {
            if !self.validate_global_multiaddr(address) {
                warn!("Attempt adding a not valid address of the peer '{}': {}", peer, address);
                return;
            }
        }
        if !self.known_peers.contains(&peer) && !addresses.is_empty() {
            self.known_peers.push(*peer);
        }
        let already_known = self.request_response.addresses_of_peer(peer);
        for address in addresses {
            if !already_known.contains(&address) {
                self.request_response.add_address(&peer, address);
            }
        }
    }

    fn maintain_known_peers(&mut self) {
        if self.known_peers.len() > MAX_PEERS {
            let mut rng = rand::thread_rng();
            let to_remove_num = self.known_peers.len() - MAX_PEERS;
            self.known_peers.shuffle(&mut rng);
            let removed_peers: Vec<_> = self.known_peers.drain(..to_remove_num).collect();
            for peer in removed_peers {
                self.forget_peer_addresses(&peer);
            }
        }
        self.request_known_peers_from_random_peer();
    }

    fn request_known_peers_from_random_peer(&mut self) {
        let mut rng = rand::thread_rng();
        if let Some(from_peer) = self.known_peers.choose(&mut rng) {
            info!("Try to request {} peers from peer {}", DEFAULT_PEERS_NUM, from_peer);
            let request = PeersExchangeRequest::GetKnownPeers { num: DEFAULT_PEERS_NUM };
            self.request_response.send_request(from_peer, request);
        }
    }

    pub fn get_random_peers(
        &mut self,
        num: usize,
        mut filter: impl FnMut(&PeerId) -> bool,
    ) -> HashMap<PeerId, PeerAddresses> {
        let mut result = HashMap::with_capacity(num);
        let mut rng = rand::thread_rng();
        let peer_ids = self.known_peers.iter().filter(|peer| filter(*peer)).collect::<Vec<_>>();
        for peer_id in peer_ids.choose_multiple(&mut rng, num) {
            let addresses = self.request_response.addresses_of_peer(*peer_id).into_iter().collect();
            result.insert(**peer_id, addresses);
        }
        result
    }

    pub fn is_known_peer(&self, peer: &PeerId) -> bool { self.known_peers.contains(peer) }

    pub fn add_known_peer(&mut self, peer: PeerId) {
        if !self.is_known_peer(&peer) {
            self.known_peers.push(peer)
        }
    }

    fn validate_global_multiaddr(&self, address: &Multiaddr) -> bool {
        let network_ports = match self.network_info {
            NetworkInfo::Distributed { network_ports } => network_ports,
            NetworkInfo::InMemory => panic!("PeersExchange must not be used with in-memory network"),
        };

        let mut components = address.iter();
        match components.next() {
            Some(Protocol::Ip4(addr)) => {
                if !addr.is_global() {
                    return false;
                }
            },
            _ => return false,
        }

        match components.next() {
            Some(Protocol::Tcp(port)) => {
                // currently, `NetworkPorts::ws` is not supported by `PeersExchange`
                if port != network_ports.tcp {
                    return false;
                }
            },
            _ => return false,
        }

        if components.next().is_some() {
            return false;
        }
        true
    }

    fn validate_get_known_peers_response(&self, response: &HashMap<PeerIdSerde, PeerAddresses>) -> bool {
        if response.is_empty() {
            return false;
        }

        if response.len() > DEFAULT_PEERS_NUM {
            return false;
        }

        for addresses in response.values() {
            if addresses.is_empty() {
                return false;
            }

            for address in addresses {
                if !self.validate_global_multiaddr(address) {
                    warn!("Received a not valid address: {}", address);
                    return false;
                }
            }
        }
        true
    }

    fn poll(
        &mut self,
        cx: &mut Context,
        _params: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction<RequestProtocol<PeersExchangeCodec>, ()>> {
        while let Poll::Ready(Some(())) = self.maintain_peers_interval.poll_next_unpin(cx) {
            self.maintain_known_peers();
        }

        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(event);
        }

        Poll::Pending
    }
}

impl NetworkBehaviourEventProcess<RequestResponseEvent<PeersExchangeRequest, PeersExchangeResponse>> for PeersExchange {
    fn inject_event(&mut self, event: RequestResponseEvent<PeersExchangeRequest, PeersExchangeResponse>) {
        match event {
            RequestResponseEvent::Message { message, peer } => match message {
                RequestResponseMessage::Request { request, channel, .. } => match request {
                    PeersExchangeRequest::GetKnownPeers { num } => {
                        let response = PeersExchangeResponse::KnownPeers {
                            peers: self.get_random_known_peers(num),
                        };
                        if let Err(_response) = self.request_response.send_response(channel, response) {
                            warn!("Response channel has been closed already");
                        }
                    },
                },
                RequestResponseMessage::Response { response, .. } => match response {
                    PeersExchangeResponse::KnownPeers { peers } => {
                        if !self.validate_get_known_peers_response(&peers) {
                            // if peer provides invalid response forget it and try to request from other peer
                            self.forget_peer(&peer);
                            self.request_known_peers_from_random_peer();
                            return;
                        }

                        info!("Got peers {:?}", peers);
                        peers.into_iter().for_each(|(peer, addresses)| {
                            self.add_peer_addresses(&peer.0, addresses);
                        });
                    },
                },
            },
            RequestResponseEvent::OutboundFailure {
                peer,
                request_id,
                error,
            } => {
                error!(
                    "Outbound failure {:?} while requesting {:?} to peer {}",
                    error, request_id, peer
                );
                self.forget_peer(&peer);
                self.request_known_peers_from_random_peer();
            },
            RequestResponseEvent::InboundFailure { peer, error, .. } => {
                error!(
                    "Inbound failure {:?} while processing request from peer {}",
                    error, peer
                );
            },
            RequestResponseEvent::ResponseSent { .. } => (),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{NetworkInfo, PeerIdSerde, PeersExchange};
    use crate::{NetworkPorts, PeerId};
    use libp2p::core::Multiaddr;
    use std::collections::{HashMap, HashSet};
    use std::iter::FromIterator;

    #[test]
    fn test_peer_id_serde() {
        let peer_id = PeerIdSerde(PeerId::random());
        let serialized = rmp_serde::to_vec(&peer_id).unwrap();
        let deserialized: PeerIdSerde = rmp_serde::from_read_ref(&serialized).unwrap();
        assert_eq!(peer_id.0, deserialized.0);
    }

    #[test]
    fn test_validate_get_known_peers_response() {
        let network_info = NetworkInfo::Distributed {
            network_ports: NetworkPorts { tcp: 3000, ws: 3010 },
        };
        let behaviour = PeersExchange::new(network_info);
        let response = HashMap::default();
        assert!(!behaviour.validate_get_known_peers_response(&response));

        let response = HashMap::from_iter(vec![(PeerIdSerde(PeerId::random()), HashSet::new())]);
        assert!(!behaviour.validate_get_known_peers_response(&response));

        let address: Multiaddr = "/ip4/127.0.0.1/tcp/3000".parse().unwrap();
        let response = HashMap::from_iter(vec![(PeerIdSerde(PeerId::random()), HashSet::from_iter(vec![address]))]);
        assert!(!behaviour.validate_get_known_peers_response(&response));

        let address: Multiaddr = "/ip4/216.58.210.142/tcp/3000".parse().unwrap();
        let response = HashMap::from_iter(vec![(PeerIdSerde(PeerId::random()), HashSet::from_iter(vec![address]))]);
        assert!(behaviour.validate_get_known_peers_response(&response));

        let address: Multiaddr = "/ip4/216.58.210.142/tcp/3000/tcp/2000".parse().unwrap();
        let response = HashMap::from_iter(vec![(PeerIdSerde(PeerId::random()), HashSet::from_iter(vec![address]))]);
        assert!(!behaviour.validate_get_known_peers_response(&response));

        let address: Multiaddr = "/ip4/216.58.210.142/tcp/3001".parse().unwrap();
        let response = HashMap::from_iter(vec![(PeerIdSerde(PeerId::random()), HashSet::from_iter(vec![address]))]);
        assert!(!behaviour.validate_get_known_peers_response(&response));

        let address: Multiaddr = "/ip4/216.58.210.142".parse().unwrap();
        let response = HashMap::from_iter(vec![(PeerIdSerde(PeerId::random()), HashSet::from_iter(vec![address]))]);
        assert!(!behaviour.validate_get_known_peers_response(&response));
    }
}
