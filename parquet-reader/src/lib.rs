use wasm_bindgen::prelude::*;
use parquet2::read;

#[wasm_bindgen]
pub fn add_two_ints(a: u32, b: u32) -> u32 {
    a + b
}
