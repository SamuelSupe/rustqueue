fn main() {
    println!("cargo:rerun-if-env-changed=RUSTQUEUE_MAX_STORAGE_FEATURE_LEVEL");
    println!("cargo:rustc-check-cfg=cfg(rustqueue_storage_feature_level_2)");
    match std::env::var("RUSTQUEUE_MAX_STORAGE_FEATURE_LEVEL")
        .unwrap_or_default()
        .trim()
    {
        "" | "2" => println!("cargo:rustc-cfg=rustqueue_storage_feature_level_2"),
        "1" => {}
        value => panic!("RUSTQUEUE_MAX_STORAGE_FEATURE_LEVEL must be 1 or 2, got {value}"),
    }
}
