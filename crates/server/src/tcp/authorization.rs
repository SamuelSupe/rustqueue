use super::*;

pub(super) async fn authorize_command(
    authenticator: Option<&Authenticator>,
    peer: SocketAddr,
    state: &mut SessionState,
    writer: &mut ClientWriter,
    command: &str,
    topic: &str,
    channel: Option<&str>,
) -> anyhow::Result<bool> {
    let Some(authenticator) = authenticator else {
        return Ok(true);
    };
    let Some(auth) = state.auth.as_ref() else {
        write_error(
            writer,
            "E_AUTH_FIRST",
            &format!("AUTH required before {command}"),
        )
        .await?;
        return Ok(false);
    };
    if auth.is_expired() {
        let secret = state.auth_secret.clone().unwrap_or_default();
        match authenticator
            .authenticate(
                &peer.ip().to_string(),
                state.encrypted,
                &state.tls_common_name,
                &secret,
            )
            .await
        {
            Ok(session) => state.auth = Some(session),
            Err(AuthError::Unauthorized) => {
                write_error(writer, "E_UNAUTHORIZED", "AUTH no authorizations found").await?;
                return Ok(false);
            }
            Err(_) => {
                write_error(writer, "E_AUTH_FAILED", "AUTH failed").await?;
                return Ok(false);
            }
        }
    }
    let allowed = match channel {
        Some(channel) => state
            .auth
            .as_ref()
            .is_some_and(|session| session.can_subscribe(topic, channel)),
        None => state
            .auth
            .as_ref()
            .is_some_and(|session| session.can_publish(topic)),
    };
    if !allowed {
        write_error(
            writer,
            "E_UNAUTHORIZED",
            &format!("AUTH failed for {command} on {topic:?} {channel:?}"),
        )
        .await?;
    }
    Ok(allowed)
}
