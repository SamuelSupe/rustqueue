use std::process::Command;

#[test]
fn unavailable_cluster_returns_an_error_without_panicking_during_tls_setup() {
    let directory = tempfile::tempdir().unwrap();
    let kubeconfig = directory.path().join("config");
    std::fs::write(
        &kubeconfig,
        r#"apiVersion: v1
kind: Config
current-context: audit
clusters:
- name: audit
  cluster:
    server: https://127.0.0.1:0
    insecure-skip-tls-verify: true
contexts:
- name: audit
  context:
    cluster: audit
    user: audit
users:
- name: audit
  user: {}
"#,
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_rustqueuectl"))
        .env("KUBECONFIG", kubeconfig)
        .env("NO_PROXY", "127.0.0.1")
        .args(["status"])
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
