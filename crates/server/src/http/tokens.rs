use crate::config::Config;
use parking_lot::RwLock;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

#[derive(Clone)]
pub(super) struct TokenSource {
    scope: &'static str,
    path: Option<Arc<PathBuf>>,
    state: Arc<RwLock<TokenState>>,
}

#[derive(Clone)]
enum TokenState {
    Unconfigured,
    Ready(Arc<str>),
    Unavailable,
}

#[derive(Clone)]
pub(super) struct TokenSet {
    pub admin: TokenSource,
    pub publish: TokenSource,
    pub registry: TokenSource,
    pub console: TokenSource,
    pub kodo_cleanup: TokenSource,
}

impl TokenSource {
    fn from_path(scope: &'static str, path: Option<&Path>) -> anyhow::Result<Self> {
        let path = path.map(|path| Arc::new(path.to_path_buf()));
        let state = match path.as_deref() {
            Some(path) => TokenState::Ready(read_token(path)?),
            None => TokenState::Unconfigured,
        };
        Ok(Self {
            scope,
            path,
            state: Arc::new(RwLock::new(state)),
        })
    }

    pub(super) fn expected(&self) -> Result<Option<Arc<str>>, &'static str> {
        match &*self.state.read() {
            TokenState::Unconfigured => Ok(None),
            TokenState::Ready(token) => Ok(Some(Arc::clone(token))),
            TokenState::Unavailable => Err(self.scope),
        }
    }

    fn refresh(&self) {
        let Some(path) = self.path.as_deref() else {
            return;
        };
        match read_token(path) {
            Ok(token) => {
                let mut state = self.state.write();
                let changed = !matches!(&*state, TokenState::Ready(current) if current.as_ref() == token.as_ref());
                *state = TokenState::Ready(token);
                if changed {
                    tracing::info!(scope = self.scope, "reloaded HTTP authorization token");
                }
            }
            Err(error) => {
                let mut state = self.state.write();
                let changed = !matches!(&*state, TokenState::Unavailable);
                *state = TokenState::Unavailable;
                if changed {
                    tracing::warn!(
                        scope = self.scope,
                        %error,
                        "HTTP authorization token became unavailable"
                    );
                }
            }
        }
    }

    #[cfg(test)]
    pub(super) fn fixed(scope: &'static str, token: &str) -> Self {
        Self {
            scope,
            path: None,
            state: Arc::new(RwLock::new(TokenState::Ready(Arc::from(token)))),
        }
    }
}

impl TokenSet {
    pub(super) fn from_config(config: &Config) -> anyhow::Result<Self> {
        Ok(Self {
            admin: TokenSource::from_path("admin", config.security.admin_token_file.as_deref())?,
            publish: TokenSource::from_path(
                "publish",
                config.security.publish_token_file.as_deref(),
            )?,
            registry: TokenSource::from_path(
                "registry",
                config.security.registry_token_file.as_deref(),
            )?,
            console: TokenSource::from_path(
                "console",
                config.security.console_token_file.as_deref(),
            )?,
            kodo_cleanup: TokenSource::from_path(
                "Kodo cleanup",
                config.security.kodo_cleanup_token_file.as_deref(),
            )?,
        })
    }

    pub(super) async fn reload(self, mut shutdown: watch::Receiver<bool>) {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
                _ = interval.tick() => {
                    self.admin.refresh();
                    self.publish.refresh();
                    self.registry.refresh();
                    self.console.refresh();
                    self.kodo_cleanup.refresh();
                }
            }
        }
    }
}

fn read_token(path: &Path) -> anyhow::Result<Arc<str>> {
    let token = std::fs::read_to_string(path)?;
    let token = token.trim();
    anyhow::ensure!(!token.is_empty(), "token file {} is empty", path.display());
    Ok(Arc::from(token))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_tokens_reload_and_fail_closed() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("token");
        std::fs::write(&path, "first\n").unwrap();
        let source = TokenSource::from_path("registry", Some(&path)).unwrap();
        assert_eq!(source.expected().unwrap().unwrap().as_ref(), "first");

        std::fs::write(&path, "second\n").unwrap();
        source.refresh();
        assert_eq!(source.expected().unwrap().unwrap().as_ref(), "second");

        std::fs::write(&path, "\n").unwrap();
        source.refresh();
        assert_eq!(source.expected(), Err("registry"));
    }

    #[tokio::test]
    async fn reload_loop_observes_rotation_and_recovers() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("token");
        std::fs::write(&path, "first\n").unwrap();
        let registry = TokenSource::from_path("registry", Some(&path)).unwrap();
        let unused = TokenSource::from_path("unused", None).unwrap();
        let tokens = TokenSet {
            admin: unused.clone(),
            publish: unused.clone(),
            registry: registry.clone(),
            console: unused.clone(),
            kodo_cleanup: unused,
        };
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let reload = tokio::spawn(tokens.reload(shutdown_rx));

        std::fs::write(&path, "second\n").unwrap();
        wait_for(&registry, Ok(Some("second"))).await;

        std::fs::write(&path, "\n").unwrap();
        wait_for(&registry, Err("registry")).await;

        std::fs::write(&path, "third\n").unwrap();
        wait_for(&registry, Ok(Some("third"))).await;

        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), reload)
            .await
            .unwrap()
            .unwrap();
    }

    async fn wait_for(source: &TokenSource, expected: Result<Option<&str>, &'static str>) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let current = source
                    .expected()
                    .map(|token| token.as_deref().map(str::to_owned));
                let expected = expected.map(|token| token.map(str::to_owned));
                if current == expected {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
}
