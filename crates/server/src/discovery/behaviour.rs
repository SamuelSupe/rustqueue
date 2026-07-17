use super::protocol::{DiscoveryRequest, DiscoveryResponse};
use libp2p::identity::Keypair;
use libp2p::swarm::behaviour::toggle::Toggle;
use libp2p::{gossipsub, identify, kad, mdns, ping, request_response, PeerId, StreamProtocol};

#[derive(libp2p::swarm::NetworkBehaviour)]
#[behaviour(to_swarm = "BehaviourEvent")]
pub struct Behaviour {
    pub identify: identify::Behaviour,
    pub ping: ping::Behaviour,
    pub request_response: request_response::cbor::Behaviour<DiscoveryRequest, DiscoveryResponse>,
    pub mdns: Toggle<mdns::tokio::Behaviour>,
    pub kademlia: kad::Behaviour<kad::store::MemoryStore>,
    pub gossipsub: gossipsub::Behaviour,
}

impl Behaviour {
    pub fn new(identity: &Keypair, mdns_enabled: bool) -> anyhow::Result<Self> {
        let public_key = identity.public();
        let peer_id = PeerId::from_public_key(&public_key);
        let mdns = if mdns_enabled {
            Some(mdns::tokio::Behaviour::new(
                mdns::Config::default(),
                peer_id,
            )?)
        } else {
            None
        };
        Ok(Self {
            identify: identify::Behaviour::new(identify::Config::new(
                "/rustqueue/identify/1.0.0".into(),
                public_key,
            )),
            ping: ping::Behaviour::default(),
            request_response: request_response::cbor::Behaviour::new(
                [(
                    StreamProtocol::new("/rustqueue/discovery/1.0.0"),
                    request_response::ProtocolSupport::Full,
                )],
                request_response::Config::default()
                    .with_request_timeout(std::time::Duration::from_secs(5)),
            ),
            mdns: mdns.into(),
            kademlia: kad::Behaviour::new(peer_id, kad::store::MemoryStore::new(peer_id)),
            gossipsub: gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(identity.clone()),
                gossipsub::ConfigBuilder::default()
                    .heartbeat_interval(std::time::Duration::from_secs(1))
                    .validation_mode(gossipsub::ValidationMode::Strict)
                    .build()
                    .map_err(|error| anyhow::anyhow!(error.to_string()))?,
            )
            .map_err(|error| anyhow::anyhow!(error.to_string()))?,
        })
    }
}

#[derive(Debug)]
pub enum BehaviourEvent {
    Identify(Box<identify::Event>),
    Ping(ping::Event),
    RequestResponse(Box<request_response::Event<DiscoveryRequest, DiscoveryResponse>>),
    Mdns(mdns::Event),
    Kademlia(Box<kad::Event>),
    Gossipsub(Box<gossipsub::Event>),
}

impl From<identify::Event> for BehaviourEvent {
    fn from(event: identify::Event) -> Self {
        Self::Identify(Box::new(event))
    }
}

impl From<ping::Event> for BehaviourEvent {
    fn from(event: ping::Event) -> Self {
        Self::Ping(event)
    }
}

impl From<request_response::Event<DiscoveryRequest, DiscoveryResponse>> for BehaviourEvent {
    fn from(event: request_response::Event<DiscoveryRequest, DiscoveryResponse>) -> Self {
        Self::RequestResponse(Box::new(event))
    }
}

impl From<mdns::Event> for BehaviourEvent {
    fn from(event: mdns::Event) -> Self {
        Self::Mdns(event)
    }
}

impl From<kad::Event> for BehaviourEvent {
    fn from(event: kad::Event) -> Self {
        Self::Kademlia(Box::new(event))
    }
}

impl From<gossipsub::Event> for BehaviourEvent {
    fn from(event: gossipsub::Event) -> Self {
        Self::Gossipsub(Box::new(event))
    }
}
