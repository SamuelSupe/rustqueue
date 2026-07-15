use hmac::{Hmac, Mac};
use libp2p::PeerId;
use rustqueue_consensus::NodeDescriptor;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::sync::Arc;

const DOMAIN: &[u8] = b"rustqueue-discovery-v2\0";
const ANNOUNCEMENT_TTL_MS: i64 = 60_000;
const MAX_CLOCK_SKEW_MS: i64 = 5_000;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AnnouncementPayload {
    pub cluster_name: String,
    pub descriptor: NodeDescriptor,
    pub peer_id: String,
    pub issued_at_ms: i64,
    pub expires_at_ms: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SignedAnnouncement {
    pub payload: AnnouncementPayload,
    pub tag: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum DiscoveryRequest {
    Hello(SignedAnnouncement),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DiscoveryResponse {
    pub accepted: bool,
    pub announcement: Option<SignedAnnouncement>,
    pub peers: Vec<PeerContact>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PeerContact {
    pub peer_id: String,
    pub addresses: Vec<String>,
    pub cell_id: rustqueue_consensus::CellId,
    pub federation_router: bool,
}

#[derive(Clone)]
pub struct Authenticator {
    cluster_name: Arc<str>,
    token: Arc<[u8]>,
}

impl Authenticator {
    pub fn new(cluster_name: impl Into<Arc<str>>, token: Vec<u8>) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            token: token.into(),
        }
    }

    pub fn sign(
        &self,
        mut descriptor: NodeDescriptor,
        peer_id: PeerId,
    ) -> anyhow::Result<SignedAnnouncement> {
        descriptor.peer_id = Some(peer_id.to_string());
        let issued_at_ms = now_ms();
        let payload = AnnouncementPayload {
            cluster_name: self.cluster_name.to_string(),
            descriptor,
            peer_id: peer_id.to_string(),
            issued_at_ms,
            expires_at_ms: issued_at_ms.saturating_add(ANNOUNCEMENT_TTL_MS),
        };
        Ok(SignedAnnouncement {
            tag: self.tag(&payload)?,
            payload,
        })
    }

    pub fn verify(
        &self,
        announcement: &SignedAnnouncement,
        remote_peer: PeerId,
    ) -> anyhow::Result<NodeDescriptor> {
        if announcement.payload.cluster_name != self.cluster_name.as_ref() {
            anyhow::bail!("peer belongs to a different cluster");
        }
        let now = now_ms();
        if announcement.payload.issued_at_ms > now.saturating_add(MAX_CLOCK_SKEW_MS)
            || announcement.payload.expires_at_ms < now
            || announcement.payload.expires_at_ms
                > announcement
                    .payload
                    .issued_at_ms
                    .saturating_add(ANNOUNCEMENT_TTL_MS)
        {
            anyhow::bail!("announcement TTL is invalid or expired");
        }
        if announcement.payload.peer_id != remote_peer.to_string()
            || announcement.payload.descriptor.peer_id.as_deref()
                != Some(announcement.payload.peer_id.as_str())
        {
            anyhow::bail!("announcement peer identity does not match the secure connection");
        }
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.token)
            .map_err(|_| anyhow::anyhow!("invalid discovery token"))?;
        mac.update(DOMAIN);
        mac.update(&serde_json::to_vec(&announcement.payload)?);
        mac.verify_slice(&announcement.tag)
            .map_err(|_| anyhow::anyhow!("announcement authentication failed"))?;
        validate_descriptor(&announcement.payload.descriptor)?;
        Ok(announcement.payload.descriptor.clone())
    }

    fn tag(&self, payload: &AnnouncementPayload) -> anyhow::Result<Vec<u8>> {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.token)
            .map_err(|_| anyhow::anyhow!("invalid discovery token"))?;
        mac.update(DOMAIN);
        mac.update(&serde_json::to_vec(payload)?);
        Ok(mac.finalize().into_bytes().to_vec())
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn validate_descriptor(descriptor: &NodeDescriptor) -> anyhow::Result<()> {
    if descriptor.id == 0
        || !descriptor.raft_address.starts_with("https://")
        || descriptor.broadcast_address.trim().is_empty()
        || descriptor.tls_server_name.trim().is_empty()
        || descriptor.failure_domain.trim().is_empty()
        || descriptor.tcp_port == 0
        || descriptor.http_port == 0
    {
        anyhow::bail!("discovered node descriptor is incomplete");
    }
    if descriptor.raft_address.len() > 512
        || descriptor.broadcast_address.len() > 255
        || descriptor.tls_server_name.len() > 253
        || descriptor.failure_domain.len() > 128
    {
        anyhow::bail!("discovered node descriptor is oversized");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p::identity::Keypair;

    fn descriptor() -> NodeDescriptor {
        NodeDescriptor {
            id: 4,
            raft_address: "https://node-4:4250".into(),
            broadcast_address: "node-4".into(),
            tcp_port: 7150,
            http_port: 7151,
            tls_server_name: "node-4".into(),
            failure_domain: "zone-4".into(),
            peer_id: None,
            cell_id: rustqueue_consensus::CellId(1),
            federation_router: false,
        }
    }

    #[test]
    fn authenticates_and_binds_peer_identity() {
        let key = Keypair::generate_ed25519();
        let peer = key.public().to_peer_id();
        let auth = Authenticator::new("cluster", vec![7; 32]);
        let signed = auth.sign(descriptor(), peer).unwrap();
        assert_eq!(auth.verify(&signed, peer).unwrap().id, 4);

        let other = Keypair::generate_ed25519().public().to_peer_id();
        assert!(auth.verify(&signed, other).is_err());
    }

    #[test]
    fn rejects_tampered_announcement() {
        let key = Keypair::generate_ed25519();
        let peer = key.public().to_peer_id();
        let auth = Authenticator::new("cluster", vec![9; 32]);
        let mut signed = auth.sign(descriptor(), peer).unwrap();
        signed.payload.descriptor.failure_domain = "other".into();
        assert!(auth.verify(&signed, peer).is_err());
    }
}
