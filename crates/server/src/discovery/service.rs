use super::behaviour::{Behaviour, BehaviourEvent};
use super::identity;
use super::protocol::{
    Authenticator, DiscoveryRequest, DiscoveryResponse, PeerContact, SignedAnnouncement,
};
use crate::config::DiscoveryConfig;
use futures::StreamExt;
use libp2p::request_response::Message;
use libp2p::swarm::SwarmEvent;
use libp2p::{
    gossipsub, identify, kad, mdns, request_response, Multiaddr, PeerId, Swarm, SwarmBuilder,
};
use rustqueue_consensus::{CellId, NodeDescriptor};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
pub struct DiscoveredNode {
    pub descriptor: NodeDescriptor,
    pub peer_id: PeerId,
    pub addresses: Vec<Multiaddr>,
    pub observed_at_ms: i64,
}

#[derive(Default)]
pub struct Directory {
    peers: parking_lot::RwLock<BTreeMap<u64, DiscoveredNode>>,
}

impl Directory {
    pub fn observe(&self, node: DiscoveredNode) {
        self.peers.write().insert(node.descriptor.id, node);
    }

    pub fn ready(&self, now_ms: i64, ttl_ms: i64) -> Vec<DiscoveredNode> {
        self.peers
            .read()
            .values()
            .filter(|node| now_ms.saturating_sub(node.observed_at_ms) <= ttl_ms)
            .cloned()
            .collect()
    }
}

pub struct Options {
    pub config: DiscoveryConfig,
    pub identity_path: PathBuf,
    pub cluster_name: String,
    pub descriptor: NodeDescriptor,
    pub join_token: Vec<u8>,
}

pub async fn run(options: Options, discovered: mpsc::Sender<DiscoveredNode>) -> anyhow::Result<()> {
    let identity = identity::load_or_create(&options.identity_path)?;
    let local_peer_id = identity.public().to_peer_id();
    let auth = Authenticator::new(options.cluster_name.clone(), options.join_token);
    let announcement = auth.sign(options.descriptor, local_peer_id)?;
    let mdns_enabled = options.config.mdns;
    let behaviour = Behaviour::new(&identity, mdns_enabled)?;
    let mut swarm = SwarmBuilder::with_existing_identity(identity)
        .with_tokio()
        .with_tcp(
            libp2p::tcp::Config::default().nodelay(true),
            libp2p::noise::Config::new,
            libp2p::yamux::Config::default,
        )?
        .with_dns()?
        .with_behaviour(move |_| behaviour)?
        .build();
    let federation_topic =
        gossipsub::IdentTopic::new(format!("rustqueue/{}/federation-v1", options.cluster_name));
    swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&federation_topic)
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let listen: Multiaddr = options.config.listen_address.parse()?;
    swarm.listen_on(listen)?;
    let seeds = parse_addresses(&options.config.seed_addresses)?;
    let mut service = Service {
        swarm,
        auth,
        announcement,
        discovered,
        known: HashMap::new(),
        topology: HashMap::new(),
        seeds,
        max_known_peers: options.config.max_known_peers,
        federation_topic,
    };
    tracing::info!(peer_id = %local_peer_id, "libp2p node discovery starting");
    service.run(options.config.announce_interval_seconds).await
}

struct Service {
    swarm: Swarm<Behaviour>,
    auth: Authenticator,
    announcement: SignedAnnouncement,
    discovered: mpsc::Sender<DiscoveredNode>,
    known: HashMap<PeerId, BTreeSet<Multiaddr>>,
    topology: HashMap<PeerId, PeerTopology>,
    seeds: Vec<Multiaddr>,
    max_known_peers: usize,
    federation_topic: gossipsub::IdentTopic,
}

#[derive(Clone, Copy, Debug)]
struct PeerTopology {
    cell_id: CellId,
    federation_router: bool,
}

impl Service {
    async fn run(&mut self, interval_seconds: u64) -> anyhow::Result<()> {
        self.dial_seeds();
        let mut announce = tokio::time::interval(Duration::from_secs(interval_seconds));
        announce.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = announce.tick() => self.announce(),
                event = self.swarm.select_next_some() => self.handle_swarm_event(event),
            }
        }
    }

    fn handle_swarm_event(&mut self, event: SwarmEvent<BehaviourEvent>) {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                tracing::info!(%address, "libp2p discovery listener ready");
            }
            SwarmEvent::ConnectionEstablished { peer_id, .. } => self.send_hello(peer_id),
            SwarmEvent::Behaviour(BehaviourEvent::Mdns(event)) => self.handle_mdns(event),
            SwarmEvent::Behaviour(BehaviourEvent::Identify(event)) => self.handle_identify(*event),
            SwarmEvent::Behaviour(BehaviourEvent::RequestResponse(event)) => {
                self.handle_request_response(*event)
            }
            SwarmEvent::Behaviour(BehaviourEvent::Ping(event)) => {
                if let Err(error) = event.result {
                    tracing::debug!(peer_id = %event.peer, %error, "libp2p peer ping failed");
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Gossipsub(event)) => {
                self.handle_gossipsub(*event)
            }
            SwarmEvent::Behaviour(BehaviourEvent::Kademlia(event)) => self.handle_kademlia(*event),
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                tracing::debug!(peer_id = ?peer_id, %error, "libp2p discovery dial failed");
            }
            _ => {}
        }
    }

    fn handle_mdns(&mut self, event: mdns::Event) {
        if let mdns::Event::Discovered(peers) = event {
            for (peer, address) in peers {
                if peer == *self.swarm.local_peer_id() {
                    continue;
                }
                self.remember(peer, address.clone());
                self.swarm.add_peer_address(peer, address.clone());
                if !self.swarm.is_connected(&peer) {
                    let _ = self.swarm.dial(address);
                }
            }
        }
    }

    fn handle_identify(&mut self, event: identify::Event) {
        if let identify::Event::Received { peer_id, info, .. } = event {
            for address in info.listen_addrs.into_iter().take(8) {
                self.remember(peer_id, address.clone());
                self.swarm.add_peer_address(peer_id, address);
            }
            self.send_hello(peer_id);
        }
    }

    fn handle_request_response(
        &mut self,
        event: request_response::Event<DiscoveryRequest, DiscoveryResponse>,
    ) {
        match event {
            request_response::Event::Message { peer, message, .. } => match message {
                Message::Request {
                    request, channel, ..
                } => {
                    let DiscoveryRequest::Hello(announcement) = request;
                    let response = match self.accept(peer, &announcement) {
                        Ok(()) => DiscoveryResponse {
                            accepted: true,
                            announcement: Some(self.announcement.clone()),
                            peers: self.peer_contacts(),
                            error: None,
                        },
                        Err(error) => DiscoveryResponse {
                            accepted: false,
                            announcement: None,
                            peers: Vec::new(),
                            error: Some(error.to_string()),
                        },
                    };
                    if self
                        .swarm
                        .behaviour_mut()
                        .request_response
                        .send_response(channel, response)
                        .is_err()
                    {
                        tracing::debug!(%peer, "discovery response channel closed");
                    }
                }
                Message::Response { response, .. } => {
                    if !response.accepted {
                        tracing::warn!(%peer, error = ?response.error, "peer rejected discovery authentication");
                        return;
                    }
                    if let Some(announcement) = response.announcement {
                        if let Err(error) = self.accept(peer, &announcement) {
                            tracing::warn!(%peer, %error, "invalid discovery response");
                            return;
                        }
                    }
                    self.import_contacts(response.peers);
                }
            },
            request_response::Event::OutboundFailure { peer, error, .. } => {
                tracing::debug!(%peer, %error, "libp2p discovery request failed");
            }
            request_response::Event::InboundFailure { peer, error, .. } => {
                tracing::debug!(%peer, %error, "libp2p discovery request failed");
            }
            request_response::Event::ResponseSent { .. } => {}
        }
    }

    fn handle_gossipsub(&mut self, event: gossipsub::Event) {
        let gossipsub::Event::Message {
            propagation_source,
            message,
            ..
        } = event
        else {
            return;
        };
        let source = message.source.unwrap_or(propagation_source);
        let Ok(announcement) = serde_json::from_slice::<SignedAnnouncement>(&message.data) else {
            return;
        };
        if let Err(error) = self.accept(source, &announcement) {
            tracing::debug!(peer_id = %source, %error, "federation gossip announcement rejected");
        }
    }

    fn handle_kademlia(&mut self, event: kad::Event) {
        if let kad::Event::RoutingUpdated { peer, .. } = event {
            tracing::trace!(%peer, "federation router table updated");
        }
    }

    fn accept(&mut self, peer: PeerId, announcement: &SignedAnnouncement) -> anyhow::Result<()> {
        let descriptor = self.auth.verify(announcement, peer)?;
        let topology = PeerTopology {
            cell_id: descriptor.cell_id,
            federation_router: descriptor.federation_router,
        };
        self.topology.insert(peer, topology);
        if !self.should_connect(topology) {
            let _ = self.swarm.disconnect_peer_id(peer);
            return Ok(());
        }
        let addresses: Vec<Multiaddr> = self
            .known
            .get(&peer)
            .map(|items| items.iter().cloned().collect())
            .unwrap_or_default();
        for address in &addresses {
            self.swarm
                .behaviour_mut()
                .kademlia
                .add_address(&peer, address.clone());
        }
        let event = DiscoveredNode {
            descriptor,
            peer_id: peer,
            addresses,
            observed_at_ms: now_ms(),
        };
        self.discovered
            .try_send(event)
            .map_err(|error| anyhow::anyhow!("discovery admission queue unavailable: {error}"))
    }

    fn send_hello(&mut self, peer: PeerId) {
        if peer == *self.swarm.local_peer_id() {
            return;
        }
        self.swarm
            .behaviour_mut()
            .request_response
            .send_request(&peer, DiscoveryRequest::Hello(self.announcement.clone()));
    }

    fn announce(&mut self) {
        if let Ok(announcement) = self.auth.sign(
            self.announcement.payload.descriptor.clone(),
            *self.swarm.local_peer_id(),
        ) {
            self.announcement = announcement;
        }
        self.dial_seeds();
        let peers: Vec<_> = self.swarm.connected_peers().copied().collect();
        for peer in peers {
            self.send_hello(peer);
        }
        if self.local_topology().federation_router {
            if let Ok(payload) = serde_json::to_vec(&self.announcement) {
                if let Err(error) = self
                    .swarm
                    .behaviour_mut()
                    .gossipsub
                    .publish(self.federation_topic.clone(), payload)
                {
                    tracing::trace!(%error, "federation route hint had no gossip peers");
                }
            }
            let _ = self.swarm.behaviour_mut().kademlia.bootstrap();
        }
    }

    fn dial_seeds(&mut self) {
        for address in self.seeds.clone() {
            let _ = self.swarm.dial(address);
        }
    }

    fn remember(&mut self, peer: PeerId, address: Multiaddr) {
        if !self.known.contains_key(&peer) && self.known.len() >= self.max_known_peers {
            return;
        }
        let addresses = self.known.entry(peer).or_default();
        if addresses.len() < 8 {
            addresses.insert(address);
        }
    }

    fn peer_contacts(&self) -> Vec<PeerContact> {
        self.known
            .iter()
            .filter_map(|(peer, addresses)| {
                let topology = self.topology.get(peer).copied()?;
                self.should_connect(topology)
                    .then_some((peer, addresses, topology))
            })
            .take(self.max_known_peers)
            .map(|(peer, addresses, topology)| PeerContact {
                peer_id: peer.to_string(),
                addresses: addresses.iter().take(8).map(ToString::to_string).collect(),
                cell_id: topology.cell_id,
                federation_router: topology.federation_router,
            })
            .collect()
    }

    fn import_contacts(&mut self, contacts: Vec<PeerContact>) {
        for contact in contacts.into_iter().take(self.max_known_peers) {
            let Ok(peer) = contact.peer_id.parse::<PeerId>() else {
                continue;
            };
            if peer == *self.swarm.local_peer_id() {
                continue;
            }
            let topology = PeerTopology {
                cell_id: contact.cell_id,
                federation_router: contact.federation_router,
            };
            if !self.should_connect(topology) {
                continue;
            }
            self.topology.insert(peer, topology);
            for address in contact.addresses.into_iter().take(8) {
                let Ok(address) = address.parse::<Multiaddr>() else {
                    continue;
                };
                self.remember(peer, address.clone());
                self.swarm
                    .behaviour_mut()
                    .kademlia
                    .add_address(&peer, address.clone());
                self.swarm.add_peer_address(peer, address);
            }
        }
    }

    fn local_topology(&self) -> PeerTopology {
        PeerTopology {
            cell_id: self.announcement.payload.descriptor.cell_id,
            federation_router: self.announcement.payload.descriptor.federation_router,
        }
    }

    fn should_connect(&self, remote: PeerTopology) -> bool {
        topology_allows(self.local_topology(), remote)
    }
}

fn topology_allows(local: PeerTopology, remote: PeerTopology) -> bool {
    remote.cell_id == local.cell_id || (local.federation_router && remote.federation_router)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn parse_addresses(values: &[String]) -> anyhow::Result<Vec<Multiaddr>> {
    values
        .iter()
        .map(|value| value.parse().map_err(anyhow::Error::from))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_nodes_stay_in_their_cell_and_only_routers_cross_cells() {
        let ordinary = PeerTopology {
            cell_id: CellId(1),
            federation_router: false,
        };
        let local_router = PeerTopology {
            cell_id: CellId(1),
            federation_router: true,
        };
        let remote_ordinary = PeerTopology {
            cell_id: CellId(2),
            federation_router: false,
        };
        let remote_router = PeerTopology {
            cell_id: CellId(2),
            federation_router: true,
        };
        assert!(topology_allows(ordinary, local_router));
        assert!(!topology_allows(ordinary, remote_router));
        assert!(!topology_allows(local_router, remote_ordinary));
        assert!(topology_allows(local_router, remote_router));
    }
}
