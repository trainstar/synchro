use std::env;
use std::fs;
use std::path::PathBuf;

use sha2::{Digest, Sha256};

fn main() {
    let version = env::var("CARGO_PKG_VERSION").expect("Cargo package version is set");
    let schema_path = PathBuf::from("sql").join(format!("synchro_pg--{version}.sql"));
    println!("cargo:rerun-if-changed={}", schema_path.display());

    let schema = fs::read(&schema_path).unwrap_or_else(|error| {
        panic!(
            "reading generated extension SQL {} failed: {error}",
            schema_path.display()
        )
    });
    let fingerprint = format!("{:x}", Sha256::digest(schema));
    let output = PathBuf::from(env::var("OUT_DIR").expect("Cargo output directory is set"))
        .join("synchro_build_fingerprint.rs");
    fs::write(
        output,
        format!("pub(crate) const SYNCHRO_BUILD_FINGERPRINT: &str = \"{fingerprint}\";\n"),
    )
    .expect("writing extension build fingerprint failed");
}
