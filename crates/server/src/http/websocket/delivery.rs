use super::{close_control, remaining_until, AppState};
use axum::extract::ws::{Message, WebSocket};
use bytes::{BufMut, BytesMut};
use rustqueue_queue::{BrokerError, Delivery, DeliveryGuard, DeliveryMode, TopicPolicy};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

const FRAME_MAGIC: &[u8; 4] = b"RQW1";
pub(super) const FRAME_HEADER_BYTES: usize = 24;

#[allow(clippy::too_many_arguments)]
pub(super) async fn deliver_one(
    socket: &mut WebSocket,
    state: &AppState,
    topic: &str,
    channel: &Arc<str>,
    delivery: Delivery,
    guard: &mut DeliveryGuard,
    policy_changes: &mut tokio::sync::watch::Receiver<TopicPolicy>,
    reliable_policy_epoch: u64,
) -> anyhow::Result<bool> {
    let Some(expiration_ns) = state
        .broker
        .delivery_expiration_ns(topic, delivery.timestamp_ns)?
    else {
        anyhow::bail!("Topic stopped using TTL_DISCARD");
    };
    if remaining_until(expiration_ns).is_zero() {
        return Ok(false);
    }
    let frame_bytes = FRAME_HEADER_BYTES.saturating_add(delivery.body.len());
    let Some(_frame_hold) = state.websocket.reserve_frame(frame_bytes) else {
        state
            .metrics
            .websocket_capacity_rejections
            .fetch_add(1, Ordering::Relaxed);
        close_control(
            socket,
            "E_WS_CAPACITY",
            "WebSocket frame memory capacity is exhausted",
            1013,
        )
        .await;
        anyhow::bail!("WebSocket frame memory capacity is exhausted");
    };
    let frame = encode_delivery(&delivery);
    let write_deadline = Instant::now()
        .checked_add(Duration::from_millis(
            state.config.limits.tcp_command_timeout_ms,
        ))
        .unwrap_or_else(Instant::now);
    let send = socket.send(Message::Binary(frame));
    tokio::pin!(send);
    let mut shutdown = state.shutdown.clone();
    loop {
        let expiration_ns = state
            .broker
            .delivery_expiration_ns(topic, delivery.timestamp_ns)?
            .ok_or_else(|| anyhow::anyhow!("Topic stopped using TTL_DISCARD"))?;
        let ttl_remaining = remaining_until(expiration_ns);
        let socket_remaining = write_deadline.saturating_duration_since(Instant::now());
        if ttl_remaining.is_zero() {
            anyhow::bail!("Message TTL expired during WebSocket frame write");
        }
        if socket_remaining.is_zero() {
            state
                .metrics
                .websocket_slow_disconnects
                .fetch_add(1, Ordering::Relaxed);
            anyhow::bail!("WebSocket frame write timed out");
        }
        let timeout = ttl_remaining.min(socket_remaining);
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                let _ = changed;
                anyhow::bail!("Broker shut down during WebSocket frame write");
            }
            changed = policy_changes.changed() => {
                if changed.is_err() {
                    anyhow::bail!("Topic closed during WebSocket frame write");
                }
                if reliable_policy_changed(state, topic, reliable_policy_epoch)? {
                    anyhow::bail!("Topic switched to RELIABLE during WebSocket frame write");
                }
                if policy_changes.borrow_and_update().delivery_mode != DeliveryMode::TtlDiscard {
                    anyhow::bail!("Topic stopped using TTL_DISCARD during WebSocket frame write");
                }
                continue;
            }
            result = &mut send => {
                result?;
                break;
            }
            _ = tokio::time::sleep(timeout) => {
                if socket_remaining <= ttl_remaining {
                    state.metrics.websocket_slow_disconnects.fetch_add(1, Ordering::Relaxed);
                    anyhow::bail!("WebSocket frame write timed out");
                }
                anyhow::bail!("Message TTL expired during WebSocket frame write");
            }
        }
    }
    let Some((token, _lease)) = guard.accept_with_lease(delivery.id) else {
        anyhow::bail!("WebSocket delivery reservation disappeared");
    };
    state
        .broker
        .finish_delivery_shared(topic, Arc::clone(channel), delivery.id, token)
        .await?;
    state
        .metrics
        .websocket_messages
        .fetch_add(1, Ordering::Relaxed);
    state
        .metrics
        .websocket_bytes
        .fetch_add(delivery.body.len() as u64, Ordering::Relaxed);
    Ok(true)
}

pub(super) fn reliable_policy_changed(
    state: &AppState,
    topic: &str,
    expected_epoch: u64,
) -> Result<bool, BrokerError> {
    Ok(state.broker.topic_reliable_policy_epoch(topic)? != expected_epoch)
}

fn encode_delivery(delivery: &Delivery) -> bytes::Bytes {
    let mut frame = BytesMut::with_capacity(FRAME_HEADER_BYTES + delivery.body.len());
    frame.extend_from_slice(FRAME_MAGIC);
    frame.put_u16(FRAME_HEADER_BYTES as u16);
    frame.put_u16(delivery.attempts);
    frame.put_i64(delivery.timestamp_ns);
    frame.put_u64(delivery.id);
    frame.extend_from_slice(&delivery.body);
    frame.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_rqw1_golden_frame() {
        let delivery = Delivery {
            id: 0x0102_0304_0506_0708,
            timestamp_ns: -0x0102_0304_0506_0708,
            attempts: 0x090a,
            body: Arc::<[u8]>::from(&b"hello"[..]),
        };

        let frame = encode_delivery(&delivery);

        assert_eq!(
            frame.as_ref(),
            &[
                b'R', b'Q', b'W', b'1', 0, 24, 9, 10, 0xfe, 0xfd, 0xfc, 0xfb, 0xfa, 0xf9, 0xf8,
                0xf8, 1, 2, 3, 4, 5, 6, 7, 8, b'h', b'e', b'l', b'l', b'o',
            ]
        );
    }
}
