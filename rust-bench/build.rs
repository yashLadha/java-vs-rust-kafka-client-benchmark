use std::env;
use std::process::Command;

fn main() {
    let rustc = env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
    let version = Command::new(rustc)
        .arg("--version")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "rustc unknown".to_string());
    println!("cargo:rustc-env=KBENCH_RUSTC_VERSION={version}");
    println!("cargo:rerun-if-changed=build.rs");
}
