use crate::config::Config;
use crate::session::SessionStore;
use crate::state::LiveState;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub client: kube::Client,
    pub http: reqwest::Client,
    pub live: Arc<LiveState>,
    pub sessions: Arc<SessionStore>,
    pub mutation_lock: Arc<Mutex<()>>,
}

impl AppState {
    pub fn new(
        config: Config,
        client: kube::Client,
        http: reqwest::Client,
        live: Arc<LiveState>,
        mutation_lock: Arc<Mutex<()>>,
    ) -> Self {
        Self {
            sessions: Arc::new(SessionStore::new(config.management_unlock)),
            config,
            client,
            http,
            live,
            mutation_lock,
        }
    }
}
