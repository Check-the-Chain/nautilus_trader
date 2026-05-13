use serde::{Deserialize, Deserializer, de};

/// Deserialize an optional i64 from either JSON number or numeric string.
pub fn opt_i64_from_string_or_number<'de, D>(de: D) -> Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    let v = Option::<serde_json::Value>::deserialize(de)?;
    Ok(match v {
        Some(serde_json::Value::Number(n)) => n.as_i64(),
        Some(serde_json::Value::String(s)) => s.parse::<i64>().ok(),
        _ => None,
    })
}

/// Deserialize an optional f64 from either JSON number or numeric string.
pub fn opt_f64_from_string_or_number<'de, D>(de: D) -> Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    let v = Option::<serde_json::Value>::deserialize(de)?;
    Ok(match v {
        Some(serde_json::Value::Number(n)) => n.as_f64(),
        Some(serde_json::Value::String(s)) => s.parse::<f64>().ok(),
        _ => None,
    })
}

/// Deserialize an optional string from either JSON string or JSON number.
pub fn opt_string_from_string_or_number<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: de::Deserializer<'de>,
{
    let val = Option::<serde_json::Value>::deserialize(deserializer)?;
    Ok(val.map(|v| match v {
        serde_json::Value::String(s) => s,
        other => other.to_string(),
    }))
}
