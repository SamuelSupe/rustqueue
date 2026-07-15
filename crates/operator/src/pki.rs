use anyhow::Context;
use rand::RngCore;
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair, KeyUsagePurpose, PublicKeyData,
};
use sha2::{Digest, Sha256};
use time::{Duration, OffsetDateTime};
use x509_parser::pem::parse_x509_pem;
use x509_parser::prelude::{FromDer, X509Certificate};

#[derive(Clone, Debug)]
pub struct CaMaterial {
    pub certificate_pem: String,
    pub private_key_pem: String,
    pub revision: u64,
    pub not_after_unix: i64,
}

#[derive(Clone, Debug)]
pub struct LeafMaterial {
    pub certificate_pem: String,
    pub private_key_pem: String,
    pub revision: u64,
    pub not_after_unix: i64,
}

pub fn generate_ca(common_name: &str, validity_days: u32) -> anyhow::Result<CaMaterial> {
    let now = OffsetDateTime::now_utc();
    let not_after = now + Duration::days(i64::from(validity_days));
    let key = KeyPair::generate().context("generate cluster CA key")?;
    let mut params = CertificateParams::new(Vec::<String>::new())?;
    params.distinguished_name = distinguished_name(common_name);
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    params.not_before = now - Duration::days(1);
    params.not_after = not_after;
    let certificate = params.self_signed(&key).context("self-sign cluster CA")?;
    let certificate_pem = certificate.pem();
    let private_key_pem = key.serialize_pem();
    Ok(CaMaterial {
        revision: revision(&[certificate_pem.as_bytes()]),
        certificate_pem,
        private_key_pem,
        not_after_unix: not_after.unix_timestamp(),
    })
}

pub fn load_ca(certificate_pem: &str, private_key_pem: &str) -> anyhow::Result<CaMaterial> {
    let key = KeyPair::from_pem(private_key_pem).context("parse cluster CA private key")?;
    let (_, pem) = parse_x509_pem(certificate_pem.as_bytes())
        .map_err(|error| anyhow::anyhow!("parse cluster CA PEM: {error}"))?;
    let (_, certificate) = X509Certificate::from_der(&pem.contents)
        .map_err(|error| anyhow::anyhow!("parse cluster CA certificate: {error}"))?;
    anyhow::ensure!(
        certificate.public_key().raw == key.subject_public_key_info(),
        "cluster CA certificate and private key do not match"
    );
    Issuer::from_ca_cert_pem(certificate_pem, &key).context("validate cluster CA certificate")?;
    let not_after_unix = certificate.validity().not_after.timestamp();
    Ok(CaMaterial {
        certificate_pem: certificate_pem.to_owned(),
        private_key_pem: private_key_pem.to_owned(),
        revision: revision(&[certificate_pem.as_bytes()]),
        not_after_unix,
    })
}

pub fn issue_leaf(
    ca: &CaMaterial,
    common_name: &str,
    dns_names: &[String],
    validity_days: u32,
) -> anyhow::Result<LeafMaterial> {
    let now = OffsetDateTime::now_utc();
    let not_after = now + Duration::days(i64::from(validity_days));
    let ca_key = KeyPair::from_pem(&ca.private_key_pem).context("parse CA key for signing")?;
    let issuer = Issuer::from_ca_cert_pem(&ca.certificate_pem, &ca_key)
        .context("parse CA certificate for signing")?;

    let key = KeyPair::generate().context("generate Broker TLS key")?;
    let mut params = CertificateParams::new(dns_names.to_vec())?;
    params.distinguished_name = distinguished_name(common_name);
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    params.not_before = now - Duration::days(1);
    params.not_after = not_after;
    let certificate = params
        .signed_by(&key, &issuer)
        .context("sign Broker TLS certificate")?;
    let certificate_pem = certificate.pem();
    let private_key_pem = key.serialize_pem();
    Ok(LeafMaterial {
        revision: revision(&[certificate_pem.as_bytes(), ca.certificate_pem.as_bytes()]),
        certificate_pem,
        private_key_pem,
        not_after_unix: not_after.unix_timestamp(),
    })
}

pub fn random_hex(bytes: usize) -> String {
    let mut value = vec![0_u8; bytes];
    rand::thread_rng().fill_bytes(&mut value);
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub fn revision(parts: &[&[u8]]) -> u64 {
    let mut digest = Sha256::new();
    for part in parts {
        digest.update(part);
    }
    let bytes = digest.finalize();
    u64::from_be_bytes(
        bytes[..8]
            .try_into()
            .expect("SHA-256 prefix is eight bytes"),
    )
}

fn distinguished_name(common_name: &str) -> DistinguishedName {
    let mut name = DistinguishedName::new();
    name.push(DnType::OrganizationName, "RustQueue");
    name.push(DnType::CommonName, common_name);
    name
}

#[cfg(test)]
mod tests {
    use super::*;
    use x509_parser::prelude::{FromDer, Pem, X509Certificate};

    #[test]
    fn generated_ca_can_sign_a_server_and_client_leaf() {
        let ca = generate_ca("test-ca", 3650).unwrap();
        let loaded = load_ca(&ca.certificate_pem, &ca.private_key_pem).unwrap();
        assert_eq!(loaded.revision, ca.revision);

        let leaf = issue_leaf(
            &loaded,
            "queue-c1-n1-0",
            &["queue-c1-n1-0.queue-cell-1.test.svc".into()],
            365,
        )
        .unwrap();
        let (pem, _) = Pem::read(std::io::Cursor::new(leaf.certificate_pem.as_bytes())).unwrap();
        let (_, certificate) = X509Certificate::from_der(&pem.contents).unwrap();
        assert!(certificate
            .subject_alternative_name()
            .unwrap()
            .unwrap()
            .value
            .general_names
            .iter()
            .any(|name| format!("{name:?}").contains("queue-c1-n1-0")));
    }

    #[test]
    fn generated_tokens_have_fixed_entropy_and_encoding() {
        let token = random_hex(32);
        assert_eq!(token.len(), 64);
        assert!(token.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }
}
