//! Endpoint End-of-Day (`/tiingo/daily/{ticker}/prices`): séries diárias de
//! ações e ETFs dos EUA.
//!
//! A Tiingo entrega, por dia, preços brutos, preços ajustados, dividendo em
//! caixa (`divCash`) e fator de split (`splitFactor`). Este módulo preserva
//! os **brutos** como inteiros exatos e os insumos de ajuste — a série
//! ajustada é responsabilidade explícita do consumidor. `adjClose` fica como
//! coluna de referência (float) para conferência, nunca como preço canônico.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Date32Builder, Float64Builder, Int64Builder, RecordBatch, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use chrono::{DateTime, NaiveDate, Utc};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::Error;

/// Versões do esquema. Mudança gera versão nova, nunca alteração in-place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaVersion {
    V1,
}

impl SchemaVersion {
    pub fn as_str(self) -> &'static str {
        match self {
            SchemaVersion::V1 => "v1",
        }
    }

    pub fn schema(self) -> Arc<Schema> {
        let ts = || DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        match self {
            SchemaVersion::V1 => Arc::new(Schema::new(vec![
                Field::new("ticker", DataType::Utf8, false),
                Field::new("refdate", DataType::Date32, false),
                // Preços brutos, 6 decimais implícitos: a era fracionária
                // pré-2001 chega da Tiingo com até 6 casas (ex.: 44.703098).
                Field::new("open_e6", DataType::Int64, false),
                Field::new("high_e6", DataType::Int64, false),
                Field::new("low_e6", DataType::Int64, false),
                Field::new("close_e6", DataType::Int64, false),
                Field::new("volume", DataType::Int64, false),
                // Insumos de ajuste do dia: valores calculados pelo provedor
                // com precisão arbitrária (dividendos de ETF chegam com 8
                // decimais) — float, não preço de bolsa.
                Field::new("div_cash", DataType::Float64, false),
                Field::new("split_factor", DataType::Float64, false),
                // Referência de conferência do provedor — nunca preço canônico.
                Field::new("adj_close_provider", DataType::Float64, false),
                Field::new("provider", DataType::Utf8, false),
                Field::new("source_file_sha256", DataType::Utf8, false),
                Field::new("ingested_at", ts(), false),
                Field::new("schema_version", DataType::Utf8, false),
            ])),
        }
    }
}

/// Diretório de cache local (raw store) das respostas.
#[derive(Debug, Clone)]
pub struct Cache {
    pub dir: PathBuf,
}

/// Resultado de um `fetch`: caminho do JSON bruto + proveniência.
#[derive(Debug, Clone)]
pub struct Fetched {
    pub path: PathBuf,
    pub sha256: String,
    pub ingested_at: DateTime<Utc>,
}

impl Fetched {
    /// Contexto de parse para o ticker requisitado (a resposta da Tiingo não
    /// repete o ticker no corpo).
    pub fn context(&self, ticker: &str) -> ParseContext {
        ParseContext {
            ticker: ticker.to_string(),
            source_file_sha256: self.sha256.clone(),
            ingested_at: self.ingested_at,
        }
    }
}

/// Proveniência de um payload já ingerido.
#[derive(Debug, Clone)]
pub struct ParseContext {
    pub ticker: String,
    pub source_file_sha256: String,
    pub ingested_at: DateTime<Utc>,
}

/// Nome (sem extensão) do arquivo de cache de uma consulta: a chave do raw
/// store é `(ticker, start, end)`.
pub fn cache_stem(ticker: &str, start: NaiveDate, end: NaiveDate) -> String {
    format!(
        "tiingo_eod_{}_{}_{}",
        ticker.to_ascii_uppercase(),
        start.format("%Y%m%d"),
        end.format("%Y%m%d")
    )
}

/// Baixa a série EOD do ticker no período e a guarda imutável no cache
/// (JSON bruto + SHA-256 + manifesto). Idempotente: com o JSON já em cache,
/// nada é baixado e a proveniência vem do manifesto.
///
/// A chave do cache é `(ticker, start, end)` — ver [`cache_stem`]. Como o
/// raw store é imutável, uma consulta com `end` no dia corrente, feita antes
/// do fechamento, fica em cache incompleta para sempre sob essa chave; para
/// séries "até hoje", use `end` = último pregão encerrado.
///
/// O token vai **somente** no header `Authorization` — nunca em URL, cache ou
/// manifesto.
#[cfg(feature = "fetch")]
pub fn fetch(
    ticker: &str,
    start: NaiveDate,
    end: NaiveDate,
    token: &str,
    cache: &Cache,
) -> Result<Fetched, Error> {
    use sha2::Digest;

    const BASE_URL: &str = "https://api.tiingo.com/tiingo/daily";

    if start > end {
        return Err(Error::InvalidRange { start, end });
    }
    let stem = cache_stem(ticker, start, end);
    let json_path = cache.dir.join(format!("{stem}.json"));
    let manifest_path = cache.dir.join(format!("{stem}.manifest.json"));

    if json_path.exists() {
        // Reconstrói a proveniência a partir do manifesto existente.
        let manifest: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&manifest_path).map_err(|source| {
                Error::Io {
                    path: manifest_path.display().to_string(),
                    source,
                }
            })?)
            .map_err(|e| Error::UnexpectedPayload(format!("manifesto inválido: {e}")))?;
        let sha256 = manifest["sha256"]
            .as_str()
            .ok_or_else(|| Error::UnexpectedPayload("manifesto sem sha256".into()))?
            .to_string();
        let ingested_at = manifest["ingested_at"]
            .as_str()
            .ok_or_else(|| Error::UnexpectedPayload("manifesto sem ingested_at".into()))?
            .parse()
            .map_err(|e| Error::UnexpectedPayload(format!("ingested_at inválido: {e}")))?;
        return Ok(Fetched {
            path: json_path,
            sha256,
            ingested_at,
        });
    }
    std::fs::create_dir_all(&cache.dir).map_err(|source| Error::Io {
        path: cache.dir.display().to_string(),
        source,
    })?;

    let url = format!(
        "{BASE_URL}/{}/prices?startDate={}&endDate={}&format=json",
        ticker.to_ascii_lowercase(),
        start.format("%Y-%m-%d"),
        end.format("%Y-%m-%d")
    );
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(300))
        .build()
        .map_err(|e| Error::Http {
            url: url.clone(),
            reason: e.to_string(),
        })?;
    let response = client
        .get(&url)
        .header("Authorization", format!("Token {token}"))
        .header("Content-Type", "application/json")
        .send()
        .map_err(|e| Error::Http {
            url: url.clone(),
            reason: e.to_string(),
        })?;
    if !response.status().is_success() {
        return Err(Error::Http {
            url,
            reason: format!("status HTTP {}", response.status()),
        });
    }
    let bytes = response.bytes().map_err(|e| Error::Http {
        url: url.clone(),
        reason: e.to_string(),
    })?;

    let sha256: String = sha2::Sha256::digest(&bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let ingested_at = Utc::now();
    let manifest = serde_json::json!({
        "provider": "tiingo",
        "endpoint": "eod_daily",
        "ticker": ticker.to_ascii_uppercase(),
        "url": url, // sem token: credencial vai apenas no header
        "file": format!("{stem}.json"),
        "sha256": sha256,
        "size_bytes": bytes.len(),
        "ingested_at": ingested_at.to_rfc3339(),
    });
    std::fs::write(
        &manifest_path,
        serde_json::to_string_pretty(&manifest).unwrap_or_default(),
    )
    .map_err(|source| Error::Io {
        path: manifest_path.display().to_string(),
        source,
    })?;

    let tmp = cache.dir.join(format!("{stem}.json.part"));
    std::fs::write(&tmp, &bytes).map_err(|source| Error::Io {
        path: tmp.display().to_string(),
        source,
    })?;
    std::fs::rename(&tmp, &json_path).map_err(|source| Error::Io {
        path: json_path.display().to_string(),
        source,
    })?;
    Ok(Fetched {
        path: json_path,
        sha256,
        ingested_at,
    })
}

/// Converte um valor decimal em inteiro com `decimals` casas implícitas.
/// Precisão além do declarado é erro — nunca arredondamento silencioso.
fn scaled_int(value: f64, decimals: u32, field: &str) -> Result<i64, Error> {
    let factor = 10f64.powi(decimals as i32);
    let scaled = value * factor;
    let rounded = scaled.round();
    // Tolerância só para ruído de float (~1e-8); um decimal real a mais
    // produziria desvio ≥ 0.1 no espaço escalado.
    if (scaled - rounded).abs() > 1e-3 {
        return Err(Error::UnexpectedPayload(format!(
            "{field} = {value} tem mais de {decimals} decimais — formato mudou?"
        )));
    }
    Ok(rounded as i64)
}

/// Parseia a resposta JSON do endpoint EOD para o esquema
/// [`SchemaVersion::V1`].
pub fn parse_bytes(bytes: &[u8], ctx: &ParseContext) -> Result<RecordBatch, Error> {
    let payload: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|e| Error::UnexpectedPayload(format!("JSON inválido: {e}")))?;
    let rows = payload
        .as_array()
        .ok_or_else(|| Error::UnexpectedPayload("resposta não é um array JSON".into()))?;

    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    let ingested_us = ctx.ingested_at.timestamp_micros();

    let mut b_ticker = StringBuilder::new();
    let mut b_refdate = Date32Builder::new();
    let mut b_open = Int64Builder::new();
    let mut b_high = Int64Builder::new();
    let mut b_low = Int64Builder::new();
    let mut b_close = Int64Builder::new();
    let mut b_volume = Int64Builder::new();
    let mut b_div = Float64Builder::new();
    let mut b_split = Float64Builder::new();
    let mut b_adj_close = Float64Builder::new();
    let mut b_provider = StringBuilder::new();
    let mut b_sha = StringBuilder::new();
    let mut b_ingested = TimestampMicrosecondBuilder::new().with_timezone("UTC");
    let mut b_schema = StringBuilder::new();

    let num = |row: &serde_json::Value, name: &str, i: usize| -> Result<f64, Error> {
        row[name].as_f64().ok_or_else(|| {
            Error::UnexpectedPayload(format!(
                "linha {i}: campo {name} ausente, nulo ou não numérico"
            ))
        })
    };

    let mut previous: Option<NaiveDate> = None;
    for (i, row) in rows.iter().enumerate() {
        let date_str = row["date"]
            .as_str()
            .ok_or_else(|| Error::UnexpectedPayload(format!("linha {i}: campo date ausente")))?;
        // "1993-01-29T00:00:00.000Z" → só a data interessa no EOD. `get` em
        // vez de slice: um byte multibyte no prefixo não pode derrubar o parser.
        let refdate = date_str
            .get(..10)
            .and_then(|d| NaiveDate::parse_from_str(d, "%Y-%m-%d").ok())
            .ok_or_else(|| {
                Error::UnexpectedPayload(format!("linha {i}: date {date_str:?} não é AAAA-MM-DD"))
            })?;
        // A Tiingo entrega em ordem crescente, um pregão por linha; repetição
        // ou retrocesso é anomalia do provedor — quarentena, não dataset.
        if let Some(prev) = previous
            && refdate <= prev
        {
            return Err(Error::UnexpectedPayload(format!(
                "linha {i}: date {refdate} não é posterior à anterior ({prev}) — duplicata ou fora de ordem"
            )));
        }
        previous = Some(refdate);

        let volume = num(row, "volume", i)?;
        if volume.fract() != 0.0 || volume < 0.0 {
            return Err(Error::UnexpectedPayload(format!(
                "linha {i}: volume {volume} não é inteiro não-negativo"
            )));
        }

        b_ticker.append_value(&ctx.ticker);
        b_refdate.append_value((refdate - epoch).num_days() as i32);
        b_open.append_value(scaled_int(num(row, "open", i)?, 6, "open")?);
        b_high.append_value(scaled_int(num(row, "high", i)?, 6, "high")?);
        b_low.append_value(scaled_int(num(row, "low", i)?, 6, "low")?);
        b_close.append_value(scaled_int(num(row, "close", i)?, 6, "close")?);
        b_volume.append_value(volume as i64);
        let div_cash = num(row, "divCash", i)?;
        let split_factor = num(row, "splitFactor", i)?;
        if div_cash < 0.0 || split_factor <= 0.0 {
            return Err(Error::UnexpectedPayload(format!(
                "linha {i}: divCash {div_cash} / splitFactor {split_factor} fora do domínio"
            )));
        }
        b_div.append_value(div_cash);
        b_split.append_value(split_factor);
        b_adj_close.append_value(num(row, "adjClose", i)?);
        b_provider.append_value("tiingo");
        b_sha.append_value(&ctx.source_file_sha256);
        b_ingested.append_value(ingested_us);
        b_schema.append_value(SchemaVersion::V1.as_str());
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(b_ticker.finish()),
        Arc::new(b_refdate.finish()),
        Arc::new(b_open.finish()),
        Arc::new(b_high.finish()),
        Arc::new(b_low.finish()),
        Arc::new(b_close.finish()),
        Arc::new(b_volume.finish()),
        Arc::new(b_div.finish()),
        Arc::new(b_split.finish()),
        Arc::new(b_adj_close.finish()),
        Arc::new(b_provider.finish()),
        Arc::new(b_sha.finish()),
        Arc::new(b_ingested.finish()),
        Arc::new(b_schema.finish()),
    ];
    RecordBatch::try_new(SchemaVersion::V1.schema(), arrays).map_err(Error::Arrow)
}

/// Grava batches em Parquet no esquema da versão indicada.
pub fn to_parquet(
    batches: &[RecordBatch],
    out: &Path,
    version: SchemaVersion,
) -> Result<(), Error> {
    let schema = version.schema();
    for batch in batches {
        if batch.schema() != schema {
            return Err(Error::Arrow(arrow::error::ArrowError::SchemaError(
                "batch não está no esquema da versão pedida".into(),
            )));
        }
    }
    let file = File::create(out).map_err(|source| Error::Io {
        path: out.display().to_string(),
        source,
    })?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.close()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Date32Array, Float64Array, Int64Array, StringArray};
    use chrono::TimeZone;

    fn ctx() -> ParseContext {
        ParseContext {
            ticker: "SPY".into(),
            source_file_sha256: "fixture-sha256".into(),
            ingested_at: Utc
                .with_ymd_and_hms(2026, 8, 21, 12, 0, 0)
                .single()
                .expect("fixo"),
        }
    }

    fn fixture() -> Vec<u8> {
        std::fs::read(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/eod_spy_sample.json"),
        )
        .expect("fixture existe")
    }

    #[test]
    fn parseia_fixture_documentada() {
        let batch = parse_bytes(&fixture(), &ctx()).expect("parseia");
        assert_eq!(batch.schema(), SchemaVersion::V1.schema());
        assert_eq!(batch.num_rows(), 4);

        let close: &Int64Array = batch
            .column_by_name("close_e6")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        // Era fracionária: 43.9375 → 43_937_500 em e6, exato.
        assert_eq!(close.value(0), 43_937_500);

        let refdate: &Date32Array = batch
            .column_by_name("refdate")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
        assert_eq!(
            epoch + chrono::Days::new(refdate.value(0) as u64),
            NaiveDate::from_ymd_opt(1993, 1, 29).expect("data")
        );

        // Dia com dividendo (precisão arbitrária do provedor).
        let div: &Float64Array = batch
            .column_by_name("div_cash")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert!((div.value(2) - 0.213).abs() < 1e-12);

        let ticker: &StringArray = batch
            .column_by_name("ticker")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert!((0..4).all(|i| ticker.value(i) == "SPY"));

        let adj: &Float64Array = batch
            .column_by_name("adj_close_provider")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert!(adj.value(3) > 0.0);
    }

    #[test]
    fn precisao_alem_de_6_decimais_no_preco_e_erro() {
        let json = br#"[{"date":"2026-08-20T00:00:00.000Z","open":1.0,"high":1.0,
            "low":1.0,"close":1.23456789,"volume":100,"divCash":0.0,
            "splitFactor":1.0,"adjClose":1.0}]"#;
        let err = parse_bytes(json, &ctx()).expect_err("deve falhar");
        assert!(matches!(err, Error::UnexpectedPayload(_)));
    }

    #[test]
    fn resposta_nao_array_e_erro() {
        let err = parse_bytes(br#"{"detail":"Invalid token."}"#, &ctx()).expect_err("deve falhar");
        assert!(matches!(err, Error::UnexpectedPayload(_)));
    }

    #[test]
    fn volume_fracionario_e_erro() {
        let json = br#"[{"date":"2026-08-20T00:00:00.000Z","open":1.0,"high":1.0,
            "low":1.0,"close":1.0,"volume":100.5,"divCash":0.0,
            "splitFactor":1.0,"adjClose":1.0}]"#;
        let err = parse_bytes(json, &ctx()).expect_err("deve falhar");
        assert!(matches!(err, Error::UnexpectedPayload(_)));
    }

    #[test]
    fn data_repetida_ou_fora_de_ordem_e_erro() {
        let row = |d: &str| {
            format!(
                r#"{{"date":"{d}T00:00:00.000Z","open":1.0,"high":1.0,"low":1.0,"close":1.0,
                "volume":1,"divCash":0.0,"splitFactor":1.0,"adjClose":1.0}}"#
            )
        };
        let dup = format!("[{},{}]", row("2026-08-20"), row("2026-08-20"));
        let err = parse_bytes(dup.as_bytes(), &ctx()).expect_err("duplicata");
        assert!(err.to_string().contains("duplicata"), "{err}");
        let desc = format!("[{},{}]", row("2026-08-21"), row("2026-08-20"));
        let err = parse_bytes(desc.as_bytes(), &ctx()).expect_err("fora de ordem");
        assert!(matches!(err, Error::UnexpectedPayload(_)));
        let ok = format!("[{},{}]", row("2026-08-20"), row("2026-08-21"));
        assert_eq!(
            parse_bytes(ok.as_bytes(), &ctx())
                .expect("ordem ok")
                .num_rows(),
            2
        );
    }

    #[test]
    fn date_malformada_e_erro_sem_panico() {
        // Prefixo com byte multibyte: um slice por bytes aqui derrubaria o parser.
        let json = r#"[{"date":"éééééé","open":1.0,"high":1.0,"low":1.0,
            "close":1.0,"volume":1,"divCash":0.0,"splitFactor":1.0,"adjClose":1.0}]"#;
        let err = parse_bytes(json.as_bytes(), &ctx()).expect_err("deve falhar");
        assert!(matches!(err, Error::UnexpectedPayload(_)));
    }

    #[test]
    fn campo_nulo_e_erro_explicito() {
        let json = br#"[{"date":"2026-08-20T00:00:00.000Z","open":null,"high":1.0,"low":1.0,
            "close":1.0,"volume":1,"divCash":0.0,"splitFactor":1.0,"adjClose":1.0}]"#;
        let err = parse_bytes(json, &ctx()).expect_err("deve falhar");
        assert!(err.to_string().contains("open"), "{err}");
    }

    #[test]
    fn nome_do_cache_e_estavel() {
        let d = |y, m, dd| NaiveDate::from_ymd_opt(y, m, dd).expect("data");
        assert_eq!(
            cache_stem("spy", d(1993, 1, 1), d(2026, 10, 28)),
            "tiingo_eod_SPY_19930101_20261028"
        );
    }

    #[cfg(feature = "fetch")]
    #[test]
    fn periodo_invertido_e_erro_antes_de_qualquer_rede() {
        let d = |y, m, dd| NaiveDate::from_ymd_opt(y, m, dd).expect("data");
        let cache = Cache {
            dir: std::env::temp_dir().join("tiingo-test-range"),
        };
        let err = fetch("SPY", d(2026, 2, 1), d(2026, 1, 1), "token-falso", &cache)
            .expect_err("deve falhar");
        assert!(matches!(err, Error::InvalidRange { .. }), "{err}");
        assert!(
            !cache.dir.exists(),
            "não deve criar cache para pedido inválido"
        );
    }

    #[test]
    fn roundtrip_parquet_preserva_esquema() {
        let batch = parse_bytes(&fixture(), &ctx()).expect("parseia");
        let dir = std::env::temp_dir().join("tiingo-test");
        std::fs::create_dir_all(&dir).expect("temp dir");
        let out = dir.join("eod_roundtrip.parquet");
        to_parquet(std::slice::from_ref(&batch), &out, SchemaVersion::V1).expect("escreve");
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            File::open(&out).expect("abre"),
        )
        .expect("reader");
        assert_eq!(
            reader.schema().as_ref(),
            SchemaVersion::V1.schema().as_ref()
        );
    }
}
