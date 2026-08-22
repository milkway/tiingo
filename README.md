# tiingo

> Cliente Rust da [Tiingo](https://www.tiingo.com) para séries EOD de ações e
> ETFs dos EUA em Arrow/Parquet — preços brutos em inteiros, política de
> ajuste explícita. *Rust client for Tiingo's end-of-day API, emitting
> Arrow/Parquet with integer raw prices and explicit adjustment policy.*

[![CI](https://github.com/milkway/tiingo/actions/workflows/ci.yml/badge.svg)](https://github.com/milkway/tiingo/actions)

A Tiingo oferece 30+ anos de histórico diário no plano gratuito (chave em
[tiingo.com](https://www.tiingo.com), limites do plano na
[documentação](https://www.tiingo.com/documentation/end-of-day)). Este crate
consome o endpoint EOD com três decisões de projeto:

1. **O esquema é o contrato.** Saída em `RecordBatch` Arrow versionado
   ([`schema/eod_daily.v1.json`](schema/eod_daily.v1.json), verificado por
   teste), sem structs de domínio próprias.
2. **Preços brutos como inteiros** com 6 decimais implícitos (`close_e6` —
   a era fracionária pré-2001 chega com até 6 casas) e **verificação de precisão**: um decimal
   inesperado é erro de formato, nunca arredondamento silencioso. Dividendos e
   splits vêm por dia (`div_cash`, `split_factor` — float, precisão arbitrária do provedor): a série ajustada é
   decisão sua, com política explícita — `adj_close_provider` (float) fica só
   como referência de conferência.
3. **Raw store imutável**: cada resposta é cacheada com SHA-256 e manifesto de
   ingestão. O token vai **somente** no header `Authorization` — nunca em URL,
   cache ou manifesto (há teste garantindo).

## Uso

```rust
use tiingo::eod::{self, Cache, SchemaVersion};

let token = std::env::var("TIINGO_API_KEY")?;
let cache = Cache { dir: "data".into() };
let fetched = eod::fetch("SPY", "1993-01-01".parse()?, "2026-08-21".parse()?, &token, &cache)?;
let batch = eod::parse_bytes(&std::fs::read(&fetched.path)?, &fetched.context("SPY"))?;
eod::to_parquet(&[batch], "spy.parquet".as_ref(), SchemaVersion::V1)?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

Ou direto pelo exemplo:

```bash
TIINGO_API_KEY=... cargo run --release --example backfill_eod -- SPY 1993-01-01 2026-08-21
```

## Testes

- Unitários e de contrato rodam offline (fixtures no formato documentado da
  API).
- `tests/live_api.rs` valida contra a API real, gated por `TIINGO_API_KEY`
  (skip limpo sem a variável; o CI não depende de credencial). Inclui teste de
  não-vazamento do token no raw store.

## Licença

MIT. Os dados são da Tiingo, sob os
[termos deles](https://www.tiingo.com/about/pricing) — este crate é só o
cliente e não redistribui dados.
