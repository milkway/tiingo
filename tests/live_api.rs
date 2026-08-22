//! Teste live contra a API real da Tiingo — gated por `TIINGO_API_KEY`
//! (skip limpo sem a variável; o CI nunca a define). Valida que o formato
//! documentado nas fixtures corresponde ao formato real.
//!
//! ```text
//! TIINGO_API_KEY=... cargo test --test live_api -- --nocapture
//! ```

use chrono::NaiveDate;

#[test]
fn fetch_e_parse_contra_a_api_real() {
    let Some(token) = tiingo::token_from_env() else {
        eprintln!("SKIP: TIINGO_API_KEY/TIINGO_TOKEN não definida — teste live não executado");
        return;
    };
    let dir = std::env::temp_dir().join("tiingo-live-test");
    let cache = tiingo::eod::Cache { dir };

    let start = NaiveDate::from_ymd_opt(2026, 8, 1).expect("data");
    let end = NaiveDate::from_ymd_opt(2026, 8, 20).expect("data");
    let fetched = tiingo::eod::fetch("SPY", start, end, &token, &cache).expect("fetch live");

    // Token jamais pode aparecer no manifesto ou no cache.
    let manifest = std::fs::read_to_string(
        fetched
            .path
            .with_extension("")
            .with_extension("manifest.json"),
    )
    .expect("manifesto existe");
    assert!(
        !manifest.contains(&token),
        "VAZAMENTO: token apareceu no manifesto"
    );

    let bytes = std::fs::read(&fetched.path).expect("json em cache");
    let batch = tiingo::eod::parse_bytes(&bytes, &fetched.context("SPY")).expect("parse live");
    assert!(
        batch.num_rows() >= 10,
        "esperava ≥10 pregões em agosto/2026"
    );
    println!(
        "live ok: {} pregões de SPY, sha256 {}",
        batch.num_rows(),
        fetched.sha256
    );
}
