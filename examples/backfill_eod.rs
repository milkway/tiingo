//! Backfill EOD de um ticker: download → cache com hash/manifesto → parse →
//! Parquet no esquema V1.
//!
//! ```text
//! TIINGO_API_KEY=... cargo run --release --example backfill_eod -- SPY 1993-01-01 2026-08-21 [dir]
//! ```

use std::path::PathBuf;

use chrono::NaiveDate;
use tiingo::eod::{self, Cache, SchemaVersion};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let token = tiingo::token_from_env().ok_or(
        "defina TIINGO_API_KEY ou TIINGO_TOKEN (a credencial nunca vai em arquivo do repo)",
    )?;
    let ticker = std::env::args().nth(1).unwrap_or_else(|| "SPY".into());
    let start: NaiveDate = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "1993-01-01".into())
        .parse()?;
    let end: NaiveDate = std::env::args()
        .nth(3)
        .unwrap_or_else(|| "2026-08-21".into())
        .parse()?;
    let dir = PathBuf::from(std::env::args().nth(4).unwrap_or_else(|| "data".into()));

    let cache = Cache { dir: dir.clone() };
    let fetched = eod::fetch(&ticker, start, end, &token, &cache)?;
    let batch = eod::parse_bytes(&std::fs::read(&fetched.path)?, &fetched.context(&ticker))?;
    let out = dir.join(format!("{}_eod.parquet", ticker.to_ascii_uppercase()));
    eod::to_parquet(std::slice::from_ref(&batch), &out, SchemaVersion::V1)?;
    println!("{ticker}: {} pregões → {}", batch.num_rows(), out.display());
    Ok(())
}
