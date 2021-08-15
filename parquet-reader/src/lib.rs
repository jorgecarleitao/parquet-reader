use std::io::Cursor;

use parquet2::read;
use serde_json::{Number, Value};

use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn read_parquet(data: Vec<u8>) -> Result<JsValue, JsValue> {
    let mut reader = Cursor::new(data);
    let version = read::read_metadata(&mut reader).map(|x| x.version);
    version
        .map(|x| Value::Number(Number::from(x)))
        .map_err(|e| JsValue::from(format!("{}", e)))
        .and_then(|x| serde_wasm_bindgen::to_value(&x).map_err(|e| JsValue::from(format!("{}", e))))
}
