use parquet2::metadata::{FileMetaData, SchemaDescriptor};
use serde::Serialize;

#[derive(Serialize)]
pub struct SchemaDescriptorDef {
    fields: Vec<String>, // names
}

impl From<SchemaDescriptor> for SchemaDescriptorDef {
    fn from(item: SchemaDescriptor) -> Self {
        Self {
            fields: item.fields().iter().map(|x| x.name().to_string()).collect(),
        }
    }
}

#[derive(Serialize)]
pub struct FileMetaDataDef {
    version: i32,
    num_rows: i64,
    created_by: Option<String>,
    schema_descr: SchemaDescriptorDef,
}

impl From<FileMetaData> for FileMetaDataDef {
    fn from(meta: FileMetaData) -> Self {
        Self {
            version: meta.version,
            num_rows: meta.num_rows,
            created_by: meta.created_by,
            schema_descr: meta.schema_descr.into(),
        }
    }
}
