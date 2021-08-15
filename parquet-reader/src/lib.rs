use std::io::Cursor;

use parquet2::read;
use wasm_bindgen::prelude::*;

mod bridge;
use bridge::*;

#[wasm_bindgen]
pub fn read_parquet(data: Vec<u8>) -> Result<JsValue, JsValue> {
    let mut reader = Cursor::new(data);
    let version = read::read_metadata(&mut reader).map(|x| {
        let x: FileMetaDataDef = x.into();
        x
    });
    version
        .map_err(|e| JsValue::from(format!("{}", e)))
        .and_then(|x| serde_json::to_value(x).map_err(|e| JsValue::from(format!("{}", e))))
        .and_then(|x| serde_wasm_bindgen::to_value(&x).map_err(|e| JsValue::from(format!("{}", e))))
}
