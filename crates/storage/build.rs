fn main() {
    println!("cargo:rerun-if-env-changed=RUSTQUEUE_MAX_STORAGE_FEATURE_LEVEL");
    println!("cargo:rustc-check-cfg=cfg(rustqueue_storage_feature_level_2)");
    if std::env::var("RUSTQUEUE_MAX_STORAGE_FEATURE_LEVEL").as_deref() == Ok("2") {
        println!("cargo:rustc-cfg=rustqueue_storage_feature_level_2");
    }
}
