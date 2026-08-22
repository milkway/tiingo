//! O esquema em código (`eod::SchemaVersion::V1`) e o contrato congelado em
//! `schema/eod_daily.v1.json` devem ser idênticos — o esquema é o contrato.

use arrow::datatypes::{DataType, TimeUnit};

#[test]
fn esquema_v1_espelha_o_contrato_congelado() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("schema/eod_daily.v1.json");
    let json = std::fs::read_to_string(&path).expect("contrato existe");
    let contract: serde_json::Value = serde_json::from_str(&json).expect("JSON válido");
    assert_eq!(contract["status"], "frozen");

    let fields = contract["fields"].as_array().expect("lista de campos");
    let schema = tiingo::eod::SchemaVersion::V1.schema();
    assert_eq!(
        schema.fields().len(),
        fields.len(),
        "quantidade de campos diverge do contrato"
    );

    for (field, spec) in schema.fields().iter().zip(fields) {
        assert_eq!(field.name(), spec["name"].as_str().expect("nome"));
        assert_eq!(
            field.is_nullable(),
            spec["nullable"].as_bool().expect("nullable"),
            "nulabilidade de {} diverge",
            field.name()
        );
        let expected = match spec["type"].as_str().expect("tipo") {
            "utf8" => DataType::Utf8,
            "date32" => DataType::Date32,
            "int32" => DataType::Int32,
            "int64" => DataType::Int64,
            "float64" => DataType::Float64,
            "timestamp_us_utc" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            other => panic!("tipo desconhecido no contrato: {other}"),
        };
        assert_eq!(
            field.data_type(),
            &expected,
            "tipo de {} diverge",
            field.name()
        );
    }
}
