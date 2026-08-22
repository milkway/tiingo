//! # tiingo — séries EOD da Tiingo em Arrow/Parquet
//!
//! Cliente do endpoint [End-of-Day da Tiingo](https://www.tiingo.com/documentation/end-of-day)
//! (ações e ETFs dos EUA, 30+ anos de histórico no plano gratuito), com três
//! decisões de projeto:
//!
//! 1. **O esquema é o contrato** (`schema/eod_daily.v1.json`, verificado por
//!    teste): a saída é um `RecordBatch` Arrow versionado, sem structs de
//!    domínio próprias.
//! 2. **Preços brutos como inteiros** com 6 decimais implícitos (`_e6`) e
//!    verificação de precisão — nunca arredondamento silencioso. Dividendos e
//!    fatores de split vêm por dia (`div_cash`, `split_factor`): a
//!    série ajustada é decisão do consumidor, com política explícita. O
//!    `adj_close_provider` (float) fica apenas como referência de conferência.
//! 3. **Raw store imutável**: cada resposta é cacheada com SHA-256 e
//!    manifesto de ingestão. O token **nunca** aparece em URL nem manifesto —
//!    vai por header `Authorization`.
//!
//! ## Exemplo
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use tiingo::eod::{self, Cache, SchemaVersion};
//!
//! let token = tiingo::token_from_env().ok_or("defina TIINGO_API_KEY ou TIINGO_TOKEN")?;
//! let cache = Cache { dir: "data".into() };
//! let fetched = eod::fetch("SPY", "1993-01-01".parse()?, "2026-08-21".parse()?, &token, &cache)?;
//! let batch = eod::parse_bytes(&std::fs::read(&fetched.path)?, &fetched.context("SPY"))?;
//! eod::to_parquet(&[batch], "spy.parquet".as_ref(), SchemaVersion::V1)?;
//! # Ok(())
//! # }
//! ```

pub mod eod;

/// Token da API a partir do ambiente: `TIINGO_API_KEY` ou, na falta,
/// `TIINGO_TOKEN`. A credencial nunca deve viver em arquivo versionado.
pub fn token_from_env() -> Option<String> {
    std::env::var("TIINGO_API_KEY")
        .or_else(|_| std::env::var("TIINGO_TOKEN"))
        .ok()
        .filter(|t| !t.trim().is_empty())
}

/// Erros do crate. Payload rejeitado nunca é silencioso; mudança de formato
/// da API é erro explícito, para quarentena — nunca descarte.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("erro de I/O em {path}: {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },
    #[error("falha na chamada à Tiingo ({url}): {reason}")]
    Http { url: String, reason: String },
    #[error("resposta da Tiingo fora do esperado: {0} — quarentena, nunca descarte silencioso")]
    UnexpectedPayload(String),
    #[error("erro Arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("erro Parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
}
