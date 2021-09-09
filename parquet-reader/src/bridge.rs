use parquet2::{
    compression::Compression as CompressionDef,
    metadata::{
        ColumnChunkMetaData as ColumnChunkMetaDataDef, FileMetaData as FileMetaDataDef,
        RowGroupMetaData as RowGroupMetaDataDef, SchemaDescriptor as SchemaDescriptorDef,
    },
    schema::{
        types::{
            BasicTypeInfo as BasicTypeInfoDef, LogicalType as LogicalTypeDef,
            ParquetType as ParquetTypeDef, PhysicalType as PhysicalTypeDef,
            TimeUnit as TimeUnitDef,
        },
        Repetition as RepetitionDef,
    },
};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub enum Repetition {
    Repeated,
    Required,
    Optional,
}

impl From<RepetitionDef> for Repetition {
    fn from(item: RepetitionDef) -> Self {
        match item {
            RepetitionDef::Optional => Repetition::Optional,
            RepetitionDef::Required => Repetition::Required,
            RepetitionDef::Repeated => Repetition::Repeated,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct BasicTypeInfo {
    name: String,
    repetition: Repetition,
}

impl From<BasicTypeInfoDef> for BasicTypeInfo {
    fn from(item: BasicTypeInfoDef) -> Self {
        Self {
            name: item.name().to_string(),
            repetition: (*item.repetition()).into(),
        }
    }
}

#[derive(Debug, Serialize)]
pub enum PhysicalType {
    Boolean,
    Int32,
    Int64,
    Int96,
    Float,
    Double,
    ByteArray,
    FixedLenByteArray(i32),
}

impl From<PhysicalTypeDef> for PhysicalType {
    fn from(item: PhysicalTypeDef) -> Self {
        match item {
            PhysicalTypeDef::Boolean => PhysicalType::Boolean,
            PhysicalTypeDef::Int32 => PhysicalType::Int32,
            PhysicalTypeDef::Int64 => PhysicalType::Int64,
            PhysicalTypeDef::Int96 => PhysicalType::Int96,
            PhysicalTypeDef::Float => PhysicalType::Float,
            PhysicalTypeDef::Double => PhysicalType::Double,
            PhysicalTypeDef::ByteArray => PhysicalType::ByteArray,
            PhysicalTypeDef::FixedLenByteArray(a) => PhysicalType::FixedLenByteArray(a),
        }
    }
}

#[derive(Debug, Serialize)]
pub enum TimeUnit {
    Miliseconds,
    Microseconds,
    Nanoseconds,
}

impl From<TimeUnitDef> for TimeUnit {
    fn from(item: TimeUnitDef) -> Self {
        match item {
            TimeUnitDef::MILLIS(_) => TimeUnit::Miliseconds,
            TimeUnitDef::NANOS(_) => TimeUnit::Nanoseconds,
            TimeUnitDef::MICROS(_) => TimeUnit::Microseconds,
        }
    }
}

impl From<LogicalTypeDef> for LogicalType {
    fn from(item: LogicalTypeDef) -> Self {
        match item {
            LogicalTypeDef::DATE(_) => LogicalType::Date,
            LogicalTypeDef::TIME(_) => LogicalType::Time,
            LogicalTypeDef::STRING(_) => LogicalType::String,
            LogicalTypeDef::INTEGER(t) => LogicalType::Integer {
                is_signed: t.is_signed,
                bits: t.bit_width,
            },
            LogicalTypeDef::TIMESTAMP(t) => LogicalType::Timestamp {
                unit: t.unit.into(),
                is_adjusted_to_utc: t.is_adjusted_to_u_t_c,
            },
            LogicalTypeDef::DECIMAL(t) => LogicalType::Decimal {
                scale: t.scale,
                precision: t.precision,
            },
            LogicalTypeDef::MAP(_) => todo!(),
            LogicalTypeDef::LIST(_) => todo!(),
            LogicalTypeDef::ENUM(_) => todo!(),
            LogicalTypeDef::UNKNOWN(_) => todo!(),
            LogicalTypeDef::JSON(_) => todo!(),
            LogicalTypeDef::BSON(_) => todo!(),
            LogicalTypeDef::UUID(_) => todo!(),
        }
    }
}

#[derive(Debug, Serialize)]
pub enum LogicalType {
    Date,
    String,
    Time,
    Integer {
        is_signed: bool,
        bits: i8,
    },
    Timestamp {
        unit: TimeUnit,
        is_adjusted_to_utc: bool,
    },
    Decimal {
        scale: i32,
        precision: i32,
    },
}

#[derive(Debug, Serialize)]
pub enum ParquetType {
    PrimitiveType {
        basic_info: BasicTypeInfo,
        logical_type: Option<LogicalType>,
        //converted_type: Option<PrimitiveConvertedType>,
        physical_type: PhysicalType,
    },
    GroupType {
        basic_info: BasicTypeInfo,
        logical_type: Option<LogicalType>,
        //converted_type: Option<GroupConvertedType>,
        fields: Vec<ParquetType>,
    },
}

impl From<ParquetTypeDef> for ParquetType {
    fn from(item: ParquetTypeDef) -> Self {
        match item {
            ParquetTypeDef::PrimitiveType {
                basic_info,
                physical_type,
                logical_type,
                ..
            } => ParquetType::PrimitiveType {
                basic_info: basic_info.into(),
                logical_type: logical_type.map(|x| x.into()),
                physical_type: physical_type.into(),
            },
            ParquetTypeDef::GroupType {
                basic_info,
                logical_type,
                fields,
                ..
            } => ParquetType::GroupType {
                basic_info: basic_info.into(),
                logical_type: logical_type.map(|x| x.into()),
                fields: fields.into_iter().map(|x| x.into()).collect(),
            },
        }
    }
}

#[derive(Debug, Serialize)]
pub struct SchemaDescriptor {
    fields: Vec<ParquetType>,
}

#[derive(Debug, Serialize)]
pub enum Compression {
    Uncompressed,
    Snappy,
    Gzip,
    Lzo,
    Brotli,
    Lz4,
    Zsld,
}

impl From<CompressionDef> for Compression {
    fn from(item: CompressionDef) -> Self {
        match item {
            CompressionDef::Uncompressed => Compression::Uncompressed,
            CompressionDef::Snappy => Compression::Snappy,
            CompressionDef::Gzip => Compression::Gzip,
            CompressionDef::Lzo => Compression::Lzo,
            CompressionDef::Brotli => Compression::Brotli,
            CompressionDef::Lz4 => Compression::Lz4,
            CompressionDef::Zsld => Compression::Zsld,
        }
    }
}

impl From<SchemaDescriptorDef> for SchemaDescriptor {
    fn from(item: SchemaDescriptorDef) -> Self {
        Self {
            fields: item.fields().iter().map(|x| x.clone().into()).collect(),
        }
    }
}

#[derive(Debug, Serialize)]
struct ColumnChunkMetaData {
    file_offset: i64,
    num_values: i64,
    compressed_size: i64,
    uncompressed_size: i64,
    byte_range: (u64, u64),
    physical_type: PhysicalType,
    compression: Compression,
}

impl From<ColumnChunkMetaDataDef> for ColumnChunkMetaData {
    fn from(item: ColumnChunkMetaDataDef) -> Self {
        Self {
            file_offset: item.file_offset(),
            num_values: item.num_values(),
            compressed_size: item.compressed_size(),
            uncompressed_size: item.uncompressed_size(),
            byte_range: item.byte_range(),
            physical_type: item.physical_type().into(),
            compression: item.compression().into(),
        }
    }
}

#[derive(Debug, Serialize)]
struct RowGroupMetaData {
    columns: Vec<ColumnChunkMetaData>,
    num_rows: i64,
    total_byte_size: i64,
}

impl From<RowGroupMetaDataDef> for RowGroupMetaData {
    fn from(item: RowGroupMetaDataDef) -> Self {
        Self {
            columns: item.columns().iter().map(|x| x.clone().into()).collect(),
            num_rows: item.num_rows(),
            total_byte_size: item.total_byte_size(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct FileMetaData {
    version: i32,
    num_rows: i64,
    created_by: Option<String>,
    schema_descr: SchemaDescriptor,
    row_groups: Vec<RowGroupMetaData>,
}

impl From<FileMetaDataDef> for FileMetaData {
    fn from(meta: FileMetaDataDef) -> Self {
        Self {
            version: meta.version,
            num_rows: meta.num_rows,
            created_by: meta.created_by,
            schema_descr: meta.schema_descr.into(),
            row_groups: meta.row_groups.into_iter().map(|x| x.into()).collect(),
        }
    }
}
