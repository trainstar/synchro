use pgrx::prelude::*;

include!(concat!(env!("OUT_DIR"), "/synchro_build_fingerprint.rs"));

pub(crate) fn library_fingerprint() -> &'static str {
    SYNCHRO_BUILD_FINGERPRINT
}

pub(crate) fn installed_fingerprint() -> Option<String> {
    Spi::connect(|client| {
        let exists: Option<bool> = client
            .select(
                "SELECT pg_catalog.to_regclass('synchro.sync_extension_build') IS NOT NULL AS exists",
                None,
                &[],
            )?
            .first()
            .get_by_name("exists")?;
        if exists != Some(true) {
            return Ok(None);
        }
        client
            .select(
                "SELECT installed_fingerprint
                 FROM synchro.sync_extension_build
                 WHERE singleton",
                None,
                &[],
            )?
            .first()
            .get_by_name("installed_fingerprint")
    })
    .ok()
    .flatten()
}

#[pg_extern]
fn synchro_build_fingerprint() -> String {
    library_fingerprint().to_string()
}
