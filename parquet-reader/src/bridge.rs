use parquet2::{
    compression::Compression,
    metadata::{ColumnChunkMetaData, FileMetaData, RowGroupMetaData, SchemaDescriptor},
    schema::{
        types::{BasicTypeInfo, LogicalType, ParquetType, PhysicalType, TimeUnit},
        Repetition,
    },
};
use serde::Serialize;

#[derive(Serialize)]
pub enum RepetitionDef {
    Repeated,
    Required,
    Optional,
}

impl From<Repetition> for RepetitionDef {
    fn from(item: Repetition) -> Self {
        match item {
            Repetition::Optional => RepetitionDef::Optional,
            Repetition::Required => RepetitionDef::Required,
            Repetition::Repeated => RepetitionDef::Repeated,
        }
    }
}

#[derive(Serialize)]
pub struct BasicTypeInfoDef {
    name: String,
    repetition: RepetitionDef,
}

impl From<BasicTypeInfo> for BasicTypeInfoDef {
    fn from(item: BasicTypeInfo) -> Self {
        Self {
            name: item.name().to_string(),
            repetition: (*item.repetition()).into(),
        }
    }
}

#[derive(Serialize)]
pub enum PhysicalTypeDef {
    Boolean,
    Int32,
    Int64,
    Int96,
    Float,
    Double,
    ByteArray,
    FixedLenByteArray(i32),
}

impl From<PhysicalType> for PhysicalTypeDef {
    fn from(item: PhysicalType) -> Self {
        match item {
            PhysicalType::Boolean => PhysicalTypeDef::Boolean,
            PhysicalType::Int32 => PhysicalTypeDef::Int32,
            PhysicalType::Int64 => PhysicalTypeDef::Int64,
            PhysicalType::Int96 => PhysicalTypeDef::Int96,
            PhysicalType::Float => PhysicalTypeDef::Float,
            PhysicalType::Double => PhysicalTypeDef::Double,
            PhysicalType::ByteArray => PhysicalTypeDef::ByteArray,
            PhysicalType::FixedLenByteArray(a) => PhysicalTypeDef::FixedLenByteArray(a),
        }
    }
}

#[derive(Serialize)]
pub enum TimeUnitDef {
    Miliseconds,
    Microseconds,
    Nanoseconds,
}

impl From<TimeUnit> for TimeUnitDef {
    fn from(item: TimeUnit) -> Self {
        match item {
            TimeUnit::MILLIS(_) => TimeUnitDef::Miliseconds,
            TimeUnit::NANOS(_) => TimeUnitDef::Nanoseconds,
            TimeUnit::MICROS(_) => TimeUnitDef::Microseconds,
        }
    }
}

impl From<LogicalType> for LogicalTypeDef {
    fn from(item: LogicalType) -> Self {
        match item {
            LogicalType::DATE(_) => LogicalTypeDef::Date,
            LogicalType::TIME(_) => LogicalTypeDef::Time,
            LogicalType::STRING(_) => LogicalTypeDef::String,
            LogicalType::INTEGER(t) => LogicalTypeDef::Integer {
                is_signed: t.is_signed,
                bits: t.bit_width,
            },
            LogicalType::TIMESTAMP(t) => LogicalTypeDef::Timestamp {
                unit: t.unit.into(),
                is_adjusted_to_utc: t.is_adjusted_to_u_t_c,
            },
            LogicalType::DECIMAL(t) => LogicalTypeDef::Decimal {
                scale: t.scale,
                precision: t.precision,
            },
            LogicalType::MAP(_) => todo!(),
            LogicalType::LIST(_) => todo!(),
            LogicalType::ENUM(_) => todo!(),
            LogicalType::UNKNOWN(_) => todo!(),
            LogicalType::JSON(_) => todo!(),
            LogicalType::BSON(_) => todo!(),
            LogicalType::UUID(_) => todo!(),
        }
    }
}

#[derive(Serialize)]
pub enum LogicalTypeDef {
    Date,
    String,
    Time,
    Integer {
        is_signed: bool,
        bits: i8,
    },
    Timestamp {
        unit: TimeUnitDef,
        is_adjusted_to_utc: bool,
    },
    Decimal {
        scale: i32,
        precision: i32,
    },
}

#[derive(Serialize)]
pub enum ParquetTypeDef {
    PrimitiveType {
        basic_info: BasicTypeInfoDef,
        logical_type: Option<LogicalTypeDef>,
        //converted_type: Option<PrimitiveConvertedType>,
        physical_type: PhysicalTypeDef,
    },
    GroupType {
        basic_info: BasicTypeInfoDef,
        //logical_type: Option<LogicalType>,
        //converted_type: Option<GroupConvertedType>,
        fields: Vec<ParquetTypeDef>,
    },
}

impl From<ParquetType> for ParquetTypeDef {
    fn from(item: ParquetType) -> Self {
        match item {
            ParquetType::PrimitiveType {
                basic_info,
                physical_type,
                logical_type,
                ..
            } => ParquetTypeDef::PrimitiveType {
                basic_info: basic_info.into(),
                logical_type: logical_type.map(|x| x.into()),
                physical_type: physical_type.into(),
            },
            ParquetType::GroupType {
                basic_info, fields, ..
            } => ParquetTypeDef::GroupType {
                basic_info: basic_info.into(),
                fields: fields.into_iter().map(|x| x.into()).collect(),
            },
        }
    }
}

#[derive(Serialize)]
pub struct SchemaDescriptorDef {
    fields: Vec<ParquetTypeDef>,
}

#[derive(Serialize)]
pub enum CompressionDef {
    Uncompressed,
    Snappy,
    Gzip,
    Lzo,
    Brotli,
    Lz4,
    Zsld,
}

impl From<Compression> for CompressionDef {
    fn from(item: Compression) -> Self {
        match item {
            Compression::Uncompressed => CompressionDef::Uncompressed,
            Compression::Snappy => CompressionDef::Snappy,
            Compression::Gzip => CompressionDef::Gzip,
            Compression::Lzo => CompressionDef::Lzo,
            Compression::Brotli => CompressionDef::Brotli,
            Compression::Lz4 => CompressionDef::Lz4,
            Compression::Zsld => CompressionDef::Zsld,
        }
    }
}

impl From<SchemaDescriptor> for SchemaDescriptorDef {
    fn from(item: SchemaDescriptor) -> Self {
        Self {
            fields: item.fields().iter().map(|x| x.clone().into()).collect(),
        }
    }
}

#[derive(Serialize)]
struct ColumnChunkMetaDataDef {
    file_offset: i64,
    num_values: i64,
    compressed_size: i64,
    uncompressed_size: i64,
    byte_range: (u64, u64),
    physical_type: PhysicalTypeDef,
    compression: CompressionDef,
}

impl From<ColumnChunkMetaData> for ColumnChunkMetaDataDef {
    fn from(item: ColumnChunkMetaData) -> Self {
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

#[derive(Serialize)]
struct RowGroupMetaDataDef {
    columns: Vec<ColumnChunkMetaDataDef>,
    num_rows: i64,
    total_byte_size: i64,
}

impl From<RowGroupMetaData> for RowGroupMetaDataDef {
    fn from(item: RowGroupMetaData) -> Self {
        Self {
            columns: item.columns().iter().map(|x| x.clone().into()).collect(),
            num_rows: item.num_rows(),
            total_byte_size: item.total_byte_size(),
        }
    }
}

#[derive(Serialize)]
pub struct FileMetaDataDef {
    version: i32,
    num_rows: i64,
    created_by: Option<String>,
    schema_descr: SchemaDescriptorDef,
    row_groups: Vec<RowGroupMetaDataDef>,
}

impl From<FileMetaData> for FileMetaDataDef {
    fn from(meta: FileMetaData) -> Self {
        Self {
            version: meta.version,
            num_rows: meta.num_rows,
            created_by: meta.created_by,
            schema_descr: meta.schema_descr.into(),
            row_groups: meta.row_groups.into_iter().map(|x| x.into()).collect(),
        }
    }
}
