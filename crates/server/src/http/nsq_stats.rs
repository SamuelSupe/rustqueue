use super::*;
use crate::subscriptions::ClientSnapshot;
use rustqueue_queue::{ChannelStats, TopicStats};

#[derive(Serialize)]
struct NsqStatsResponse {
    version: &'static str,
    health: &'static str,
    start_time: i64,
    topics: Vec<NsqTopicStats>,
}

#[derive(Serialize)]
struct NsqTopicStats {
    topic_name: String,
    name: String,
    depth: u64,
    memory_depth: u64,
    backend_depth: u64,
    message_count: u64,
    paused: bool,
    channels: Vec<NsqChannelStats>,
}

#[derive(Serialize)]
struct NsqChannelStats {
    channel_name: String,
    name: String,
    depth: u64,
    memory_depth: u64,
    backend_depth: u64,
    message_count: u64,
    in_flight_count: u64,
    deferred_count: u64,
    requeue_count: u64,
    timeout_count: u64,
    client_count: usize,
    clients: Vec<ClientSnapshot>,
    paused: bool,
}

pub(super) async fn stats(
    State(state): State<AppState>,
    Query(query): Query<StatsQuery>,
) -> Response {
    if let Err(error) = state.broker.expire_in_flight().await {
        return ApiError::from(error).into_response();
    }
    let filtered = state
        .broker
        .filtered_stats(query.topic.as_deref(), query.channel.as_deref());
    let include_clients = query.include_clients.unwrap_or(true);
    if query.format.as_deref() == Some("json") {
        let topics = filtered
            .topics
            .into_iter()
            .map(|topic| convert_topic(&state, topic, include_clients))
            .collect();
        return Json(NsqStatsResponse {
            version: env!("CARGO_PKG_VERSION"),
            health: "OK",
            start_time: state.started_at,
            topics,
        })
        .into_response();
    }
    text_stats(filtered).into_response()
}

fn convert_topic(state: &AppState, topic: TopicStats, include_clients: bool) -> NsqTopicStats {
    let topic_name = topic.name;
    let channels = topic
        .channels
        .into_iter()
        .map(|channel| convert_channel(state, &topic_name, channel, include_clients))
        .collect();
    NsqTopicStats {
        topic_name: topic_name.clone(),
        name: topic_name,
        depth: nsq_topic_depth(),
        memory_depth: 0,
        backend_depth: 0,
        message_count: topic.published_count,
        paused: topic.paused,
        channels,
    }
}

fn convert_channel(
    state: &AppState,
    topic: &str,
    channel: ChannelStats,
    include_clients: bool,
) -> NsqChannelStats {
    let queued_depth = nsq_channel_depth(&channel);
    let channel_name = channel.name;
    let (client_count, clients) = if include_clients {
        let clients = state.subscriptions.clients(topic, &channel_name);
        (clients.len(), clients)
    } else {
        (
            state.subscriptions.client_count(topic, &channel_name),
            Vec::new(),
        )
    };
    NsqChannelStats {
        channel_name: channel_name.clone(),
        name: channel_name,
        depth: queued_depth,
        memory_depth: 0,
        backend_depth: queued_depth,
        message_count: channel.message_count,
        in_flight_count: channel.in_flight_count,
        deferred_count: channel.deferred_count,
        requeue_count: channel.requeue_count,
        timeout_count: channel.timeout_count,
        client_count,
        clients,
        paused: channel.paused,
    }
}

fn nsq_channel_depth(channel: &ChannelStats) -> u64 {
    channel
        .depth
        .saturating_sub(channel.in_flight_count)
        .saturating_sub(channel.deferred_count)
}

fn nsq_topic_depth() -> u64 {
    // RustQueue exposes every durable log position directly to each Channel;
    // there is no intermediate NSQ Topic queue waiting to fan messages out.
    0
}

fn text_stats(stats: BrokerStats) -> String {
    let mut output = String::new();
    for topic in stats.topics {
        output.push_str(&format!("[{}] depth={}\n", topic.name, nsq_topic_depth()));
        for channel in topic.channels {
            output.push_str(&format!(
                "   [{}] depth={} in-flight={} deferred={}\n",
                channel.name,
                nsq_channel_depth(&channel),
                channel.in_flight_count,
                channel.deferred_count
            ));
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_channel_fields_use_nsq_names_and_keep_name_alias() {
        let value = serde_json::to_value(NsqChannelStats {
            channel_name: "workers".into(),
            name: "workers".into(),
            depth: 4,
            memory_depth: 0,
            backend_depth: 4,
            message_count: 7,
            in_flight_count: 1,
            deferred_count: 2,
            requeue_count: 3,
            timeout_count: 5,
            client_count: 0,
            clients: Vec::new(),
            paused: false,
        })
        .unwrap();
        assert_eq!(value["channel_name"], "workers");
        assert_eq!(value["name"], "workers");
        assert_eq!(value["message_count"], 7);
        assert_eq!(value["client_count"], 0);
    }

    #[test]
    fn nsq_depth_excludes_in_flight_and_deferred_messages() {
        let channel = ChannelStats {
            name: "workers".into(),
            depth: 7,
            message_count: 9,
            in_flight_count: 2,
            deferred_count: 3,
            requeue_count: 0,
            timeout_count: 0,
            paused: false,
            ephemeral: false,
            ack_cursor: 0,
            ack_gap: 0,
        };
        assert_eq!(nsq_channel_depth(&channel), 2);
    }

    #[test]
    fn retained_topic_messages_are_not_reported_as_nsq_fanout_depth() {
        assert_eq!(nsq_topic_depth(), 0);
    }

    #[test]
    fn kodo_replay_fixture_matches_the_rustqueue_stats_contract() {
        let actual = serde_json::to_value(NsqStatsResponse {
            version: "0.8.0",
            health: "OK",
            start_time: 1_700_000_001,
            topics: vec![NsqTopicStats {
                topic_name: "events".into(),
                name: "events".into(),
                depth: 0,
                memory_depth: 0,
                backend_depth: 0,
                message_count: 10,
                paused: false,
                channels: vec![NsqChannelStats {
                    channel_name: "workers".into(),
                    name: "workers".into(),
                    depth: 4,
                    memory_depth: 0,
                    backend_depth: 4,
                    message_count: 10,
                    in_flight_count: 1,
                    deferred_count: 2,
                    requeue_count: 3,
                    timeout_count: 4,
                    client_count: 0,
                    clients: Vec::new(),
                    paused: false,
                }],
            }],
        })
        .unwrap();
        let expected: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../tests/kodo-replay/fixtures/stats-1.json"
        ))
        .unwrap();
        assert_eq!(actual, expected);
    }
}
