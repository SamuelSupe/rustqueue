use crate::config::TlsConfig;
use anyhow::{bail, Context};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{RootCertStore, ServerConfig};
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tokio_rustls::TlsAcceptor;
use x509_parser::prelude::{FromDer, X509Certificate};

pub fn acceptor(config: Option<&TlsConfig>) -> anyhow::Result<Option<TlsAcceptor>> {
    let Some(config) = config else {
        return Ok(None);
    };
    Ok(Some(TlsAcceptor::from(server_config(config)?)))
}

pub fn server_config(config: &TlsConfig) -> anyhow::Result<Arc<ServerConfig>> {
    let certificates = load_certificates(&config.certificate_file)?;
    let private_key = load_private_key(&config.private_key_file)?;
    let builder = ServerConfig::builder();
    let mut server = if config.require_client_certificate {
        let ca_path = config
            .client_ca_file
            .as_ref()
            .context("client CA is required for mTLS")?;
        let mut roots = RootCertStore::empty();
        for certificate in load_certificates(ca_path)? {
            roots
                .add(certificate)
                .context("add client CA certificate")?;
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .context("build client certificate verifier")?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certificates, private_key)
            .context("build mTLS server configuration")?
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(certificates, private_key)
            .context("build TLS server configuration")?
    };
    server.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(Arc::new(server))
}

pub fn peer_common_name(stream: &tokio_rustls::server::TlsStream<tokio::net::TcpStream>) -> String {
    stream
        .get_ref()
        .1
        .peer_certificates()
        .and_then(|certificates| certificates.first())
        .and_then(|certificate| X509Certificate::from_der(certificate.as_ref()).ok())
        .and_then(|(_, certificate)| {
            certificate
                .subject()
                .iter_common_name()
                .next()
                .and_then(|name| name.as_str().ok())
                .map(str::to_owned)
        })
        .unwrap_or_default()
}

pub fn internal_http_client(config: &TlsConfig) -> anyhow::Result<reqwest::Client> {
    build_internal_http_client(config, std::time::Duration::from_secs(10))
}

pub fn internal_snapshot_http_client(config: &TlsConfig) -> anyhow::Result<reqwest::Client> {
    build_internal_http_client(config, std::time::Duration::from_secs(60))
}

fn build_internal_http_client(
    config: &TlsConfig,
    request_timeout: std::time::Duration,
) -> anyhow::Result<reqwest::Client> {
    let mut identity_pem = std::fs::read(&config.certificate_file).with_context(|| {
        format!(
            "read internal certificate {}",
            config.certificate_file.display()
        )
    })?;
    identity_pem.extend_from_slice(b"\n");
    identity_pem.extend_from_slice(&std::fs::read(&config.private_key_file).with_context(
        || {
            format!(
                "read internal private key {}",
                config.private_key_file.display()
            )
        },
    )?);
    let identity =
        reqwest::Identity::from_pem(&identity_pem).context("parse internal TLS client identity")?;
    let root_path = config
        .root_ca_file
        .as_ref()
        .context("internal TLS root_ca_file is required")?;
    let root = reqwest::Certificate::from_pem(
        &std::fs::read(root_path)
            .with_context(|| format!("read internal root CA {}", root_path.display()))?,
    )
    .context("parse internal root CA")?;
    reqwest::Client::builder()
        .identity(identity)
        .add_root_certificate(root)
        .https_only(true)
        .http2_prior_knowledge()
        .http2_keep_alive_interval(std::time::Duration::from_secs(10))
        .http2_keep_alive_timeout(std::time::Duration::from_secs(3))
        .http2_keep_alive_while_idle(true)
        .pool_idle_timeout(std::time::Duration::from_secs(90))
        .pool_max_idle_per_host(1)
        .connect_timeout(std::time::Duration::from_secs(2))
        .timeout(request_timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("build internal HTTP/2 mTLS client")
}

fn load_certificates(path: &std::path::Path) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    let mut reader = BufReader::new(
        File::open(path).with_context(|| format!("open certificate {}", path.display()))?,
    );
    let certificates: Vec<_> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<_, _>>()
        .with_context(|| format!("parse certificate {}", path.display()))?;
    if certificates.is_empty() {
        bail!(
            "certificate file {} contains no certificates",
            path.display()
        );
    }
    Ok(certificates)
}

fn load_private_key(path: &std::path::Path) -> anyhow::Result<PrivateKeyDer<'static>> {
    let mut reader = BufReader::new(
        File::open(path).with_context(|| format!("open private key {}", path.display()))?,
    );
    rustls_pemfile::private_key(&mut reader)
        .with_context(|| format!("parse private key {}", path.display()))?
        .with_context(|| format!("private key file {} contains no key", path.display()))
}
