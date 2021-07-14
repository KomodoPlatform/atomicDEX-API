use crate::{adex_ping::AdexPing,
            network::{get_all_network_seednodes, NETID_7777},
            peers_exchange::{PeerAddresses, PeersExchange},
            request_response::{build_request_response_behaviour, PeerRequest, PeerResponse, RequestResponseBehaviour,
                               RequestResponseBehaviourEvent, RequestResponseSender},
            runtime::{SwarmRuntimeOps, SWARM_RUNTIME},
            NetworkInfo, NetworkPorts, RelayAddress, RelayAddressError};
use atomicdex_gossipsub::{Gossipsub, GossipsubConfigBuilder, GossipsubEvent, GossipsubMessage, MessageId, Topic,
                          TopicHash};
use derive_more::Display;
use futures::{channel::{mpsc::{channel, Receiver, Sender},
                        oneshot},
              future::{abortable, join_all, poll_fn, AbortHandle},
              Future, SinkExt, StreamExt};
use libp2p::core::transport::Boxed as BoxedTransport;
use libp2p::swarm::{IntoProtocolsHandler, NetworkBehaviour, ProtocolsHandler};
use libp2p::{core::{ConnectedPoint, Multiaddr, Transport},
             identity,
             multiaddr::Protocol,
             noise,
             request_response::ResponseChannel,
             swarm::{ExpandedSwarm, NetworkBehaviourEventProcess, Swarm},
             NetworkBehaviour, PeerId};
use libp2p_floodsub::{Floodsub, FloodsubEvent, Topic as FloodsubTopic};
use log::{debug, error, info};
use rand::seq::SliceRandom;
use rand::Rng;
use std::{collections::hash_map::{DefaultHasher, HashMap},
          hash::{Hash, Hasher},
          iter,
          net::IpAddr,
          task::{Context, Poll},
          time::Duration};
use void::Void;
use wasm_timer::{Instant, Interval};

pub type AdexCmdTx = Sender<AdexBehaviourCmd>;
pub type AdexEventRx = Receiver<AdexBehaviourEvent>;

#[cfg(test)] mod tests;

pub const PEERS_TOPIC: &str = "PEERS";
const CONNECTED_RELAYS_CHECK_INTERVAL: Duration = Duration::from_secs(30);
const ANNOUNCE_INTERVAL: Duration = Duration::from_secs(600);
const ANNOUNCE_INITIAL_DELAY: Duration = Duration::from_secs(60);
const CHANNEL_BUF_SIZE: usize = 1024 * 8;

/// Returns info about connected peers
pub async fn get_peers_info(mut cmd_tx: AdexCmdTx) -> HashMap<String, Vec<String>> {
    let (result_tx, rx) = oneshot::channel();
    let cmd = AdexBehaviourCmd::GetPeersInfo { result_tx };
    cmd_tx.send(cmd).await.expect("Rx should be present");
    rx.await.expect("Tx should be present")
}

/// Returns current gossipsub mesh state
pub async fn get_gossip_mesh(mut cmd_tx: AdexCmdTx) -> HashMap<String, Vec<String>> {
    let (result_tx, rx) = oneshot::channel();
    let cmd = AdexBehaviourCmd::GetGossipMesh { result_tx };
    cmd_tx.send(cmd).await.expect("Rx should be present");
    rx.await.expect("Tx should be present")
}

pub async fn get_gossip_peer_topics(mut cmd_tx: AdexCmdTx) -> HashMap<String, Vec<String>> {
    let (result_tx, rx) = oneshot::channel();
    let cmd = AdexBehaviourCmd::GetGossipPeerTopics { result_tx };
    cmd_tx.send(cmd).await.expect("Rx should be present");
    rx.await.expect("Tx should be present")
}

pub async fn get_gossip_topic_peers(mut cmd_tx: AdexCmdTx) -> HashMap<String, Vec<String>> {
    let (result_tx, rx) = oneshot::channel();
    let cmd = AdexBehaviourCmd::GetGossipTopicPeers { result_tx };
    cmd_tx.send(cmd).await.expect("Rx should be present");
    rx.await.expect("Tx should be present")
}

pub async fn get_relay_mesh(mut cmd_tx: AdexCmdTx) -> Vec<String> {
    let (result_tx, rx) = oneshot::channel();
    let cmd = AdexBehaviourCmd::GetRelayMesh { result_tx };
    cmd_tx.send(cmd).await.expect("Rx should be present");
    rx.await.expect("Tx should be present")
}

#[derive(Debug)]
pub struct AdexResponseChannel(ResponseChannel<PeerResponse>);

impl From<ResponseChannel<PeerResponse>> for AdexResponseChannel {
    fn from(res: ResponseChannel<PeerResponse>) -> Self { AdexResponseChannel(res) }
}

impl From<AdexResponseChannel> for ResponseChannel<PeerResponse> {
    fn from(res: AdexResponseChannel) -> Self { res.0 }
}

#[derive(Debug)]
pub enum AdexBehaviourCmd {
    Subscribe {
        /// Subscribe to this topic
        topic: String,
    },
    PublishMsg {
        topics: Vec<String>,
        msg: Vec<u8>,
    },
    /// Request relays sequential until a response is received.
    RequestAnyRelay {
        req: Vec<u8>,
        response_tx: oneshot::Sender<Option<(PeerId, Vec<u8>)>>,
    },
    /// Request given peers and collect all their responses.
    RequestPeers {
        req: Vec<u8>,
        peers: Vec<String>,
        response_tx: oneshot::Sender<Vec<(PeerId, AdexResponse)>>,
    },
    /// Request relays and collect all their responses.
    RequestRelays {
        req: Vec<u8>,
        response_tx: oneshot::Sender<Vec<(PeerId, AdexResponse)>>,
    },
    /// Send a response using a `response_channel`.
    SendResponse {
        /// Response to a request.
        res: AdexResponse,
        /// Pass the same `response_channel` as that was obtained from [`AdexBehaviourEvent::PeerRequest`].
        response_channel: AdexResponseChannel,
    },
    GetPeersInfo {
        result_tx: oneshot::Sender<HashMap<String, Vec<String>>>,
    },
    GetGossipMesh {
        result_tx: oneshot::Sender<HashMap<String, Vec<String>>>,
    },
    GetGossipPeerTopics {
        result_tx: oneshot::Sender<HashMap<String, Vec<String>>>,
    },
    GetGossipTopicPeers {
        result_tx: oneshot::Sender<HashMap<String, Vec<String>>>,
    },
    GetRelayMesh {
        result_tx: oneshot::Sender<Vec<String>>,
    },
    PropagateMessage {
        message_id: MessageId,
        propagation_source: PeerId,
    },
}

/// The structure is the same as `PeerResponse`,
/// but is used to prevent `PeerResponse` from being used outside the network implementation.
#[derive(Debug, Eq, PartialEq)]
pub enum AdexResponse {
    Ok { response: Vec<u8> },
    None,
    Err { error: String },
}

impl From<PeerResponse> for AdexResponse {
    fn from(res: PeerResponse) -> Self {
        match res {
            PeerResponse::Ok { res } => AdexResponse::Ok { response: res },
            PeerResponse::None => AdexResponse::None,
            PeerResponse::Err { err } => AdexResponse::Err { error: err },
        }
    }
}

impl From<AdexResponse> for PeerResponse {
    fn from(res: AdexResponse) -> Self {
        match res {
            AdexResponse::Ok { response } => PeerResponse::Ok { res: response },
            AdexResponse::None => PeerResponse::None,
            AdexResponse::Err { error } => PeerResponse::Err { err: error },
        }
    }
}

/// The structure consists of GossipsubEvent and RequestResponse events.
/// It is used to prevent the network events from being used outside the network implementation.
#[derive(Debug)]
pub enum AdexBehaviourEvent {
    /// A message has been received.
    /// Derived from GossipsubEvent.
    Message(PeerId, MessageId, GossipsubMessage),
    /// A remote subscribed to a topic.
    Subscribed {
        /// Remote that has subscribed.
        peer_id: PeerId,
        /// The topic it has subscribed to.
        topic: TopicHash,
    },
    /// A remote unsubscribed from a topic.
    Unsubscribed {
        /// Remote that has unsubscribed.
        peer_id: PeerId,
        /// The topic it has subscribed from.
        topic: TopicHash,
    },
    /// A remote peer sent a request and waits for a response.
    PeerRequest {
        /// Remote that sent this request.
        peer_id: PeerId,
        /// The serialized data.
        request: Vec<u8>,
        /// A channel for sending a response to this request.
        /// The channel is used to identify the peer on the network that is waiting for an answer to this request.
        /// See [`AdexBehaviourCmd::SendResponse`].
        response_channel: AdexResponseChannel,
    },
}

impl From<GossipsubEvent> for AdexBehaviourEvent {
    fn from(event: GossipsubEvent) -> Self {
        match event {
            GossipsubEvent::Message(peer_id, message_id, gossipsub_message) => {
                AdexBehaviourEvent::Message(peer_id, message_id, gossipsub_message)
            },
            GossipsubEvent::Subscribed { peer_id, topic } => AdexBehaviourEvent::Subscribed { peer_id, topic },
            GossipsubEvent::Unsubscribed { peer_id, topic } => AdexBehaviourEvent::Unsubscribed { peer_id, topic },
        }
    }
}

/// AtomicDEX libp2p Network behaviour implementation
#[derive(NetworkBehaviour)]
pub struct AtomicDexBehaviour {
    #[behaviour(ignore)]
    event_tx: Sender<AdexBehaviourEvent>,
    #[behaviour(ignore)]
    spawn_fn: fn(Box<dyn Future<Output = ()> + Send + Unpin + 'static>) -> (),
    #[behaviour(ignore)]
    cmd_rx: Receiver<AdexBehaviourCmd>,
    #[behaviour(ignore)]
    netid: u16,
    floodsub: Floodsub,
    gossipsub: Gossipsub,
    request_response: RequestResponseBehaviour,
    peers_exchange: PeersExchange,
    ping: AdexPing,
}

impl AtomicDexBehaviour {
    fn notify_on_adex_event(&mut self, event: AdexBehaviourEvent) {
        if let Err(e) = self.event_tx.try_send(event) {
            error!("notify_on_adex_event error {}", e);
        }
    }

    fn spawn(&self, fut: impl Future<Output = ()> + Send + 'static) { (self.spawn_fn)(Box::new(Box::pin(fut))) }

    fn process_cmd(&mut self, cmd: AdexBehaviourCmd) {
        match cmd {
            AdexBehaviourCmd::Subscribe { topic } => {
                let topic = Topic::new(topic);
                self.gossipsub.subscribe(topic);
            },
            AdexBehaviourCmd::PublishMsg { topics, msg } => {
                self.gossipsub.publish_many(topics.into_iter().map(Topic::new), msg);
            },
            AdexBehaviourCmd::RequestAnyRelay { req, response_tx } => {
                let relays = self.gossipsub.get_relay_mesh();
                // spawn the `request_any_peer` future
                let future = request_any_peer(relays, req, self.request_response.sender(), response_tx);
                self.spawn(future);
            },
            AdexBehaviourCmd::RequestPeers {
                req,
                peers,
                response_tx,
            } => {
                let peers = peers
                    .into_iter()
                    .filter_map(|peer| match peer.parse() {
                        Ok(p) => Some(p),
                        Err(e) => {
                            error!("Error on parse peer id {:?}: {:?}", peer, e);
                            None
                        },
                    })
                    .collect();
                let future = request_peers(peers, req, self.request_response.sender(), response_tx);
                self.spawn(future);
            },
            AdexBehaviourCmd::RequestRelays { req, response_tx } => {
                let relays = self.gossipsub.get_relay_mesh();
                // spawn the `request_peers` future
                let future = request_peers(relays, req, self.request_response.sender(), response_tx);
                self.spawn(future);
            },
            AdexBehaviourCmd::SendResponse { res, response_channel } => {
                if let Err(response) = self.request_response.send_response(response_channel.into(), res.into()) {
                    error!("Error sending response: {:?}", response);
                }
            },
            AdexBehaviourCmd::GetPeersInfo { result_tx } => {
                let result = self
                    .gossipsub
                    .get_peers_connections()
                    .into_iter()
                    .map(|(peer_id, connected_points)| {
                        let peer_id = peer_id.to_base58();
                        let connected_points = connected_points
                            .into_iter()
                            .map(|(_conn_id, point)| match point {
                                ConnectedPoint::Dialer { address } => address.to_string(),
                                ConnectedPoint::Listener { send_back_addr, .. } => send_back_addr.to_string(),
                            })
                            .collect();
                        (peer_id, connected_points)
                    })
                    .collect();
                if result_tx.send(result).is_err() {
                    debug!("Result rx is dropped");
                }
            },
            AdexBehaviourCmd::GetGossipMesh { result_tx } => {
                let result = self
                    .gossipsub
                    .get_mesh()
                    .iter()
                    .map(|(topic, peers)| {
                        let topic = topic.to_string();
                        let peers = peers.iter().map(|peer| peer.to_string()).collect();
                        (topic, peers)
                    })
                    .collect();
                if result_tx.send(result).is_err() {
                    debug!("Result rx is dropped");
                }
            },
            AdexBehaviourCmd::GetGossipPeerTopics { result_tx } => {
                let result = self
                    .gossipsub
                    .get_all_peer_topics()
                    .iter()
                    .map(|(peer, topics)| {
                        let peer = peer.to_string();
                        let topics = topics.iter().map(|topic| topic.to_string()).collect();
                        (peer, topics)
                    })
                    .collect();
                if result_tx.send(result).is_err() {
                    error!("Result rx is dropped");
                }
            },
            AdexBehaviourCmd::GetGossipTopicPeers { result_tx } => {
                let result = self
                    .gossipsub
                    .get_all_topic_peers()
                    .iter()
                    .map(|(topic, peers)| {
                        let topic = topic.to_string();
                        let peers = peers.iter().map(|peer| peer.to_string()).collect();
                        (topic, peers)
                    })
                    .collect();
                if result_tx.send(result).is_err() {
                    error!("Result rx is dropped");
                }
            },
            AdexBehaviourCmd::GetRelayMesh { result_tx } => {
                let result = self
                    .gossipsub
                    .get_relay_mesh()
                    .into_iter()
                    .map(|peer| peer.to_string())
                    .collect();
                if result_tx.send(result).is_err() {
                    error!("Result rx is dropped");
                }
            },
            AdexBehaviourCmd::PropagateMessage {
                message_id,
                propagation_source,
            } => {
                self.gossipsub.propagate_message(&message_id, &propagation_source);
            },
        }
    }

    fn announce_listeners(&mut self, listeners: PeerAddresses) {
        let serialized = rmp_serde::to_vec(&listeners).expect("PeerAddresses serialization should never fail");
        self.floodsub.publish(FloodsubTopic::new(PEERS_TOPIC), serialized);
    }

    pub fn connected_relays_len(&self) -> usize { self.gossipsub.connected_relays_len() }

    pub fn relay_mesh_len(&self) -> usize { self.gossipsub.relay_mesh_len() }

    pub fn received_messages_in_period(&self) -> (Duration, usize) { self.gossipsub.get_received_messages_in_period() }

    pub fn connected_peers_len(&self) -> usize { self.gossipsub.get_num_peers() }
}

impl NetworkBehaviourEventProcess<GossipsubEvent> for AtomicDexBehaviour {
    fn inject_event(&mut self, event: GossipsubEvent) { self.notify_on_adex_event(event.into()); }
}

impl NetworkBehaviourEventProcess<FloodsubEvent> for AtomicDexBehaviour {
    fn inject_event(&mut self, event: FloodsubEvent) {
        // do not process peer announce on 7777 temporary
        if self.netid != NETID_7777 {
            if let FloodsubEvent::Message(message) = &event {
                for topic in &message.topics {
                    if topic == &FloodsubTopic::new(PEERS_TOPIC) {
                        let addresses: PeerAddresses = match rmp_serde::from_read_ref(&message.data) {
                            Ok(a) => a,
                            Err(_) => return,
                        };
                        self.peers_exchange.add_peer_addresses(&message.source, addresses);
                    }
                }
            }
        }
    }
}

impl NetworkBehaviourEventProcess<Void> for AtomicDexBehaviour {
    fn inject_event(&mut self, _event: Void) {}
}

impl NetworkBehaviourEventProcess<()> for AtomicDexBehaviour {
    fn inject_event(&mut self, _event: ()) {}
}

impl NetworkBehaviourEventProcess<RequestResponseBehaviourEvent> for AtomicDexBehaviour {
    fn inject_event(&mut self, event: RequestResponseBehaviourEvent) {
        match event {
            RequestResponseBehaviourEvent::InboundRequest {
                peer_id,
                request,
                response_channel,
            } => {
                let event = AdexBehaviourEvent::PeerRequest {
                    peer_id,
                    request: request.req,
                    response_channel: response_channel.into(),
                };
                // forward the event to the AdexBehaviourCmd handler
                self.notify_on_adex_event(event);
            },
        }
    }
}

/// Custom types mapping the complex associated types of AtomicDexBehaviour to the ExpandedSwarm
type AdexSwarmHandler = <<AtomicDexBehaviour as NetworkBehaviour>::ProtocolsHandler as IntoProtocolsHandler>::Handler;
type AtomicDexSwarm = ExpandedSwarm<
    AtomicDexBehaviour,
    <AdexSwarmHandler as ProtocolsHandler>::InEvent,
    <AdexSwarmHandler as ProtocolsHandler>::OutEvent,
    <AtomicDexBehaviour as NetworkBehaviour>::ProtocolsHandler,
>;

fn maintain_connection_to_relays(swarm: &mut AtomicDexSwarm, bootstrap_addresses: &[Multiaddr]) {
    let behaviour = swarm.behaviour();
    let connected_relays = behaviour.gossipsub.connected_relays();
    let mesh_n_low = behaviour.gossipsub.get_config().mesh_n_low;
    let mesh_n = behaviour.gossipsub.get_config().mesh_n;
    // allow 2 * mesh_n_high connections to other nodes
    let max_n = behaviour.gossipsub.get_config().mesh_n_high * 2;

    let mut rng = rand::thread_rng();
    if connected_relays.len() < mesh_n_low {
        let to_connect_num = mesh_n - connected_relays.len();
        let to_connect = swarm
            .behaviour_mut()
            .peers_exchange
            .get_random_peers(to_connect_num, |peer| !connected_relays.contains(peer));

        // choose some random bootstrap addresses to connect if peers exchange returned not enough peers
        if to_connect.len() < to_connect_num {
            let connect_bootstrap_num = to_connect_num - to_connect.len();
            for addr in bootstrap_addresses
                .iter()
                .filter(|addr| !swarm.behaviour().gossipsub.is_connected_to_addr(addr))
                .collect::<Vec<_>>()
                .choose_multiple(&mut rng, connect_bootstrap_num)
            {
                if let Err(e) = libp2p::Swarm::dial_addr(swarm, (*addr).clone()) {
                    error!("Bootstrap addr {} dial error {}", addr, e);
                }
            }
        }
        for (peer, addresses) in to_connect {
            for addr in addresses {
                if swarm.behaviour().gossipsub.is_connected_to_addr(&addr) {
                    continue;
                }
                if let Err(e) = libp2p::Swarm::dial_addr(swarm, addr.clone()) {
                    error!("Peer {} address {} dial error {}", peer, addr, e);
                }
            }
        }
    }

    if connected_relays.len() > max_n {
        let to_disconnect_num = connected_relays.len() - max_n;
        let relays_mesh = swarm.behaviour().gossipsub.get_relay_mesh();
        let not_in_mesh: Vec<_> = connected_relays
            .iter()
            .filter(|peer| !relays_mesh.contains(peer))
            .collect();
        for peer in not_in_mesh.choose_multiple(&mut rng, to_disconnect_num) {
            info!("Disconnecting peer {}", peer);
            if Swarm::disconnect_peer_id(swarm, **peer).is_err() {
                error!("Peer {} disconnect error", peer);
            }
        }
    }

    for relay in connected_relays {
        if !swarm.behaviour().peers_exchange.is_known_peer(&relay) {
            swarm.behaviour_mut().peers_exchange.add_known_peer(relay);
        }
    }
}

fn announce_my_addresses(swarm: &mut AtomicDexSwarm) {
    let global_listeners: PeerAddresses = Swarm::listeners(&swarm)
        .filter(|listener| {
            for protocol in listener.iter() {
                if let Protocol::Ip4(ip) = protocol {
                    return ip.is_global();
                }
            }
            false
        })
        .take(1)
        .cloned()
        .collect();
    if !global_listeners.is_empty() {
        swarm.behaviour_mut().announce_listeners(global_listeners);
    }
}

#[derive(Debug, Display)]
pub enum AdexBehaviourError {
    #[display(fmt = "{}", _0)]
    ParsingRelayAddress(RelayAddressError),
}

impl From<RelayAddressError> for AdexBehaviourError {
    fn from(e: RelayAddressError) -> Self { AdexBehaviourError::ParsingRelayAddress(e) }
}

pub enum NodeType {
    Light { network_ports: NetworkPorts },
    LightInMemory,
    Relay { ip: IpAddr, network_ports: NetworkPorts },
    RelayInMemory { port: u64 },
}

impl NodeType {
    pub fn to_network_info(&self) -> NetworkInfo {
        match self {
            NodeType::Light { network_ports } | NodeType::Relay { network_ports, .. } => NetworkInfo::Distributed {
                network_ports: *network_ports,
            },
            NodeType::LightInMemory | NodeType::RelayInMemory { .. } => NetworkInfo::InMemory,
        }
    }
}

impl NodeType {
    pub fn is_relay(&self) -> bool { matches!(self, NodeType::Relay { .. } | NodeType::RelayInMemory { .. }) }
}

/// Creates and spawns new AdexBehaviour Swarm returning:
/// 1. tx to send control commands
/// 2. rx emitting gossip events to processing side
/// 3. our peer_id
/// 4. abort handle to stop the P2P processing fut.
pub async fn spawn_gossipsub(
    netid: u16,
    force_key: Option<[u8; 32]>,
    spawn_fn: fn(Box<dyn Future<Output = ()> + Send + Unpin + 'static>) -> (),
    to_dial: Vec<RelayAddress>,
    node_type: NodeType,
    on_poll: impl Fn(&AtomicDexSwarm) + Send + 'static,
) -> Result<(Sender<AdexBehaviourCmd>, AdexEventRx, PeerId, AbortHandle), AdexBehaviourError> {
    let (result_tx, result_rx) = futures::channel::oneshot::channel();
    let fut = async move {
        let result = start_gossipsub(netid, force_key, spawn_fn, to_dial, node_type, on_poll);
        result_tx.send(result).unwrap();
    };

    // `Libp2p` must be spawned on the tokio runtime
    SWARM_RUNTIME.spawn(fut);
    result_rx.await.expect("Fatal error on starting gossipsub")
}

/// Creates and spawns new AdexBehaviour Swarm returning:
/// 1. tx to send control commands
/// 2. rx emitting gossip events to processing side
/// 3. our peer_id
/// 4. abort handle to stop the P2P processing fut
///
/// Prefer using [`spawn_gossipsub`] to make sure the Swarm is initialized and spawned on the same runtime.
/// Otherwise, you can face the following error:
/// `panicked at 'there is no reactor running, must be called from the context of a Tokio 1.x runtime'`.
#[allow(clippy::too_many_arguments)]
fn start_gossipsub(
    netid: u16,
    force_key: Option<[u8; 32]>,
    spawn_fn: fn(Box<dyn Future<Output = ()> + Send + Unpin + 'static>) -> (),
    to_dial: Vec<RelayAddress>,
    node_type: NodeType,
    on_poll: impl Fn(&AtomicDexSwarm) + Send + 'static,
) -> Result<(Sender<AdexBehaviourCmd>, AdexEventRx, PeerId, AbortHandle), AdexBehaviourError> {
    let i_am_relay = node_type.is_relay();
    let mut rng = rand::thread_rng();
    let local_key = generate_ed25519_keypair(&mut rng, force_key);
    let local_peer_id = PeerId::from(local_key.public());
    info!("Local peer id: {:?}", local_peer_id);

    let noise_keys = noise::Keypair::<noise::X25519Spec>::new()
        .into_authentic(&local_key)
        .expect("Signing libp2p-noise static DH keypair failed.");

    let network_info = node_type.to_network_info();
    let transport = match network_info {
        NetworkInfo::InMemory => build_memory_transport(noise_keys),
        NetworkInfo::Distributed { .. } => build_dns_ws_transport(noise_keys),
    };

    let (cmd_tx, cmd_rx) = channel(CHANNEL_BUF_SIZE);
    let (event_tx, event_rx) = channel(CHANNEL_BUF_SIZE);

    let bootstrap = to_dial
        .into_iter()
        .map(|addr| addr.try_to_multiaddr(network_info))
        .collect::<Result<Vec<Multiaddr>, _>>()?;

    let (mesh_n_low, mesh_n, mesh_n_high) = if i_am_relay { (3, 6, 8) } else { (2, 3, 4) };

    // Create a Swarm to manage peers and events
    let mut swarm = {
        // to set default parameters for gossipsub use:
        // let gossipsub_config = gossipsub::GossipsubConfig::default();

        // To content-address message, we can take the hash of message and use it as an ID.
        let message_id_fn = |message: &GossipsubMessage| {
            let mut s = DefaultHasher::new();
            message.data.hash(&mut s);
            message.sequence_number.hash(&mut s);
            MessageId(s.finish().to_string())
        };

        // set custom gossipsub
        let gossipsub_config = GossipsubConfigBuilder::new()
            .message_id_fn(message_id_fn)
            .i_am_relay(i_am_relay)
            .mesh_n_low(mesh_n_low)
            .mesh_n(mesh_n)
            .mesh_n_high(mesh_n_high)
            .manual_propagation()
            .max_transmit_size(1024 * 1024 - 100)
            .build();
        // build a gossipsub network behaviour
        let mut gossipsub = Gossipsub::new(local_peer_id, gossipsub_config);

        let floodsub = Floodsub::new(local_peer_id, netid != NETID_7777);

        let mut peers_exchange = PeersExchange::new(network_info);
        if !network_info.in_memory() {
            // Please note WASM nodes don't support `PeersExchange` currently,
            // so `get_all_network_seednodes` returns an empty list.
            for (peer_id, addr) in get_all_network_seednodes(netid) {
                let multiaddr = addr.try_to_multiaddr(network_info)?;
                peers_exchange.add_peer_addresses(&peer_id, iter::once(multiaddr).collect());
                gossipsub.add_explicit_relay(peer_id);
            }
        }

        // build a request-response network behaviour
        let request_response = build_request_response_behaviour();

        // use default ping config with 15s interval, 20s timeout and 1 max failure
        let ping = AdexPing::new();

        let adex_behavior = AtomicDexBehaviour {
            event_tx,
            spawn_fn,
            cmd_rx,
            gossipsub,
            floodsub,
            request_response,
            peers_exchange,
            ping,
            netid,
        };
        libp2p::swarm::SwarmBuilder::new(transport, adex_behavior, local_peer_id)
            .executor(Box::new(&*SWARM_RUNTIME))
            .build()
    };
    swarm
        .behaviour_mut()
        .floodsub
        .subscribe(FloodsubTopic::new(PEERS_TOPIC.to_owned()));

    match node_type {
        NodeType::Relay { ip, network_ports } => {
            let dns_addr: Multiaddr = format!("/ip4/{}/tcp/{}", ip, network_ports.tcp).parse().unwrap();
            let ws_addr: Multiaddr = format!("/ip4/{}/tcp/{}/ws", ip, network_ports.ws).parse().unwrap();
            libp2p::Swarm::listen_on(&mut swarm, dns_addr).unwrap();
            libp2p::Swarm::listen_on(&mut swarm, ws_addr).unwrap();
        },
        NodeType::RelayInMemory { port } => {
            let memory_addr: Multiaddr = format!("/memory/{}", port).parse().unwrap();
            libp2p::Swarm::listen_on(&mut swarm, memory_addr).unwrap();
        },
        _ => (),
    }

    for relay in bootstrap.choose_multiple(&mut rng, mesh_n) {
        match libp2p::Swarm::dial_addr(&mut swarm, relay.clone()) {
            Ok(_) => info!("Dialed {}", relay),
            Err(e) => error!("Dial {:?} failed: {:?}", relay, e),
        }
    }

    let mut check_connected_relays_interval = Interval::new_at(
        Instant::now() + CONNECTED_RELAYS_CHECK_INTERVAL,
        CONNECTED_RELAYS_CHECK_INTERVAL,
    );
    let mut announce_interval = Interval::new_at(Instant::now() + ANNOUNCE_INITIAL_DELAY, ANNOUNCE_INTERVAL);
    let mut listening = false;
    let polling_fut = poll_fn(move |cx: &mut Context| {
        loop {
            match swarm.behaviour_mut().cmd_rx.poll_next_unpin(cx) {
                Poll::Ready(Some(cmd)) => swarm.behaviour_mut().process_cmd(cmd),
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => break,
            }
        }

        loop {
            match swarm.poll_next_unpin(cx) {
                Poll::Ready(Some(event)) => debug!("Swarm event {:?}", event),
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => break,
            }
        }

        if swarm.behaviour().gossipsub.is_relay() {
            while let Poll::Ready(Some(())) = announce_interval.poll_next_unpin(cx) {
                announce_my_addresses(&mut swarm);
            }
        }

        while let Poll::Ready(Some(())) = check_connected_relays_interval.poll_next_unpin(cx) {
            maintain_connection_to_relays(&mut swarm, &bootstrap);
        }

        if !listening && i_am_relay {
            for listener in Swarm::listeners(&swarm) {
                info!("Listening on {}", listener);
                listening = true;
            }
        }
        on_poll(&swarm);
        Poll::Pending
    });

    let (polling_fut, abort_handle) = abortable(polling_fut);
    SWARM_RUNTIME.spawn(polling_fut);

    Ok((cmd_tx, event_rx, local_peer_id, abort_handle))
}

#[cfg(target_arch = "wasm32")]
fn build_dns_ws_transport(
    noise_keys: libp2p::noise::AuthenticKeypair<libp2p::noise::X25519Spec>,
) -> BoxedTransport<(PeerId, libp2p::core::muxing::StreamMuxerBox)> {
    let websocket = libp2p::wasm_ext::ffi::websocket_transport();
    let transport = libp2p::wasm_ext::ExtTransport::new(websocket);
    upgrade_transport(transport, noise_keys)
}

#[cfg(not(target_arch = "wasm32"))]
fn build_dns_ws_transport(
    noise_keys: libp2p::noise::AuthenticKeypair<libp2p::noise::X25519Spec>,
) -> BoxedTransport<(PeerId, libp2p::core::muxing::StreamMuxerBox)> {
    let tcp = libp2p::tcp::TokioTcpConfig::new().nodelay(true);
    let dns_tcp =
        libp2p::dns::TokioDnsConfig::custom(tcp, libp2p::dns::ResolverConfig::google(), Default::default()).unwrap();
    let ws_dns_tcp = libp2p::websocket::WsConfig::new(dns_tcp.clone());
    let transport = dns_tcp.or_transport(ws_dns_tcp);
    upgrade_transport(transport, noise_keys)
}

fn build_memory_transport(
    noise_keys: libp2p::noise::AuthenticKeypair<libp2p::noise::X25519Spec>,
) -> BoxedTransport<(PeerId, libp2p::core::muxing::StreamMuxerBox)> {
    let transport = libp2p::core::transport::MemoryTransport::default();
    upgrade_transport(transport, noise_keys)
}

/// Set up an encrypted Transport over the Mplex protocol.
fn upgrade_transport<T>(
    transport: T,
    noise_keys: libp2p::noise::AuthenticKeypair<libp2p::noise::X25519Spec>,
) -> BoxedTransport<(PeerId, libp2p::core::muxing::StreamMuxerBox)>
where
    T: Transport + Clone + Send + Sync + 'static,
    T::Output: futures::AsyncRead + futures::AsyncWrite + Unpin + Send + 'static,
    T::ListenerUpgrade: Send,
    T::Listener: Send,
    T::Dial: Send,
    T::Error: Send + Sync + 'static,
{
    transport
        .upgrade(libp2p::core::upgrade::Version::V1)
        .authenticate(noise::NoiseConfig::xx(noise_keys).into_authenticated())
        .multiplex(libp2p::mplex::MplexConfig::default())
        .timeout(std::time::Duration::from_secs(20))
        .map(|(peer, muxer), _| (peer, libp2p::core::muxing::StreamMuxerBox::new(muxer)))
        .boxed()
}

fn generate_ed25519_keypair<R: Rng>(rng: &mut R, force_key: Option<[u8; 32]>) -> identity::Keypair {
    let mut raw_key = match force_key {
        Some(key) => key,
        None => {
            let mut key = [0; 32];
            rng.fill_bytes(&mut key);
            key
        },
    };
    let secret = identity::ed25519::SecretKey::from_bytes(&mut raw_key).expect("Secret length is 32 bytes");
    let keypair = identity::ed25519::Keypair::from(secret);
    identity::Keypair::Ed25519(keypair)
}

/// Request the peers sequential until a `PeerResponse::Ok()` will not be received.
async fn request_any_peer(
    peers: Vec<PeerId>,
    request_data: Vec<u8>,
    request_response_tx: RequestResponseSender,
    response_tx: oneshot::Sender<Option<(PeerId, Vec<u8>)>>,
) {
    debug!("start request_any_peer loop: peers {}", peers.len());
    for peer in peers {
        match request_one_peer(peer, request_data.clone(), request_response_tx.clone()).await {
            PeerResponse::Ok { res } => {
                debug!("Received a response from peer {:?}, stop the request loop", peer);
                if response_tx.send(Some((peer, res))).is_err() {
                    error!("Response oneshot channel was closed");
                }
                return;
            },
            PeerResponse::None => {
                debug!("Received None from peer {:?}, request next peer", peer);
            },
            PeerResponse::Err { err } => {
                error!("Error on request {:?} peer: {:?}. Request next peer", peer, err);
            },
        };
    }

    debug!("None of the peers responded to the request");
    if response_tx.send(None).is_err() {
        error!("Response oneshot channel was closed");
    };
}

/// Request the peers and collect all their responses.
async fn request_peers(
    peers: Vec<PeerId>,
    request_data: Vec<u8>,
    request_response_tx: RequestResponseSender,
    response_tx: oneshot::Sender<Vec<(PeerId, AdexResponse)>>,
) {
    debug!("start request_any_peer loop: peers {}", peers.len());
    let mut futures = Vec::with_capacity(peers.len());
    for peer in peers {
        let request_data = request_data.clone();
        let request_response_tx = request_response_tx.clone();
        futures.push(async move {
            let response = request_one_peer(peer, request_data, request_response_tx).await;
            (peer, response)
        })
    }

    let responses = join_all(futures)
        .await
        .into_iter()
        .map(|(peer_id, res)| {
            let res: AdexResponse = res.into();
            (peer_id, res)
        })
        .collect();

    if response_tx.send(responses).is_err() {
        error!("Response oneshot channel was closed");
    };
}

async fn request_one_peer(peer: PeerId, req: Vec<u8>, mut request_response_tx: RequestResponseSender) -> PeerResponse {
    // Use the internal receiver to receive a response to this request.
    let (internal_response_tx, internal_response_rx) = oneshot::channel();
    let request = PeerRequest { req };
    request_response_tx
        .send((peer, request, internal_response_tx))
        .await
        .unwrap();

    match internal_response_rx.await {
        Ok(response) => response,
        Err(e) => PeerResponse::Err {
            err: format!("Error on request the peer {:?}: \"{:?}\". Request next peer", peer, e),
        },
    }
}
