use std::path::PathBuf;
use std::process::Command;

pub fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..")
}

/// Integration tests spawn the controller with `--cert-path certs/cert.pem`.
/// The CE tree does not ship those files; generate a throwaway pair when missing.
pub fn ensure_test_certs() {
    let cert_dir = workspace_root().join("certs");
    let cert = cert_dir.join("cert.pem");
    let key = cert_dir.join("key.pem");
    if cert.is_file() && key.is_file() {
        return;
    }
    std::fs::create_dir_all(&cert_dir).expect("create certs/ for controller tests");
    let status = Command::new("openssl")
        .args([
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-keyout",
            key.to_str().expect("key path utf-8"),
            "-out",
            cert.to_str().expect("cert path utf-8"),
            "-days",
            "1",
            "-nodes",
            "-subj",
            "/CN=localhost",
        ])
        .status()
        .expect("openssl is required to mint lab TLS certs for controller tests");
    assert!(
        status.success(),
        "openssl failed to write certs/cert.pem and certs/key.pem"
    );
}
