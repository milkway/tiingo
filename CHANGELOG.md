# Changelog

Formato: [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/); versões
seguem [SemVer](https://semver.org/lang/pt-BR/). O esquema Arrow (`v1`) tem
versionamento próprio e **não mudou**: Parquets gerados pela 0.1.0 continuam
válidos.

## [0.2.0] — 2026-10-29

### Corrigido

- **Payload com datas repetidas ou fora de ordem passava em silêncio.** A
  Tiingo entrega um pregão por linha, em ordem crescente; agora repetição ou
  retrocesso de data é `Error::UnexpectedPayload` (quarentena), em vez de
  entrar no dataset como linha duplicada.
- `date` com byte multibyte no prefixo derrubava o parser por slice de bytes
  fora de fronteira de caractere; agora é erro de payload.
- `start > end` em `fetch` agora falha antes de qualquer rede ou escrita em
  cache (`Error::InvalidRange`), em vez de chamar a API com período invertido.
- Mensagem de campo numérico ausente distingue "ausente, nulo ou não
  numérico" (um `null` do provedor era reportado como "ausente").
- `tools/run-with-env.sh` estava sem bit de execução no git.

### Alterado

- **`arrow`/`parquet` 59 → 60.** Como os tipos do Arrow fazem parte da API
  pública (`RecordBatch`), quem consome o crate precisa acompanhar — daí a
  versão 0.2. MSRV 1.85 → 1.88 (exigida pelo arrow 60).
- `parquet` sem features default: só `arrow` + `snap` (o único codec usado).
  Saem `zstd`, `brotli`, `lz4` e `flate2` da árvore; `cargo audit` passa de
  13 vulnerabilidades para zero.
- `Error` ganhou a variante `InvalidRange` (quebra para `match` exaustivo).

### Adicionado

- Feature `fetch` (default), como no `rsb3`: desligada, o crate é só o parser
  e não traz `reqwest`/`sha2`.
- `eod::cache_stem(ticker, start, end)`: a chave do raw store, pública, para
  quem precisa localizar o JSON em cache sem chamar `fetch`.
- Documentado o efeito colateral do raw store imutável: `end` no dia corrente
  antes do fechamento fica em cache incompleto para sempre sob aquela chave.
- Validado contra a API real em 2026-10-29: histórico completo do SPY (8.495
  pregões desde 1993) sem nulls, no máximo 6 decimais nos preços, volume
  inteiro, datas ordenadas e únicas — as premissas do parser continuam de pé.
- CI: jobs de MSRV e `cargo audit` (também semanal), `cargo doc -D warnings`,
  clippy e testes também sem features default.

## [0.1.0] — 2026-08-22

Primeira versão: cliente EOD em Arrow/Parquet com preços brutos `_e6`, insumos
de ajuste por dia e raw store com SHA-256 e manifesto.
