//! Typed schema vocabulary shared by SQL and Graph.
//!
//! Nested `Array` / `Struct` / `Map` may be *declared* so the validator can
//! reject them consistently. Hot-path kernels in M0 only handle the scalar
//! variants plus `Dynamic`.

use crate::error::{ErrorCode, Result, SparrowError};
use crate::ids::{FieldId, SchemaId};

/// Logical field type. Keep the hot path on the first-class scalars.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum DataType {
    Null,
    Bool,
    Int64,
    UInt64,
    Float64,
    Utf8,
    Bytes,
    /// Microseconds since Unix epoch, UTC. No timezone payload.
    TimestampMicrosUTC,
    /// Heterogeneous JSON-like value. Arithmetic on Dynamic without CAST is
    /// rejected at the G0/binder gate.
    Dynamic,
    /// Declared only; not a V0.1 hot-path type.
    Array(Box<DataType>),
    /// Declared only; not a V0.1 hot-path type.
    Struct(Vec<Field>),
    /// Declared only; not a V0.1 hot-path type.
    Map {
        key: Box<DataType>,
        value: Box<DataType>,
    },
}

impl DataType {
    pub fn is_nested(&self) -> bool {
        matches!(self, Self::Array(_) | Self::Struct(_) | Self::Map { .. })
    }

    pub fn is_numeric(&self) -> bool {
        matches!(self, Self::Int64 | Self::UInt64 | Self::Float64)
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Bool => "bool",
            Self::Int64 => "int64",
            Self::UInt64 => "uint64",
            Self::Float64 => "float64",
            Self::Utf8 => "utf8",
            Self::Bytes => "bytes",
            Self::TimestampMicrosUTC => "timestamp_micros_utc",
            Self::Dynamic => "dynamic",
            Self::Array(_) => "array",
            Self::Struct(_) => "struct",
            Self::Map { .. } => "map",
        }
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Array(inner) => write!(f, "array<{inner}>"),
            Self::Struct(fields) => {
                write!(f, "struct<")?;
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", field.name, field.data_type)?;
                }
                write!(f, ">")
            }
            Self::Map { key, value } => write!(f, "map<{key}, {value}>"),
            other => f.write_str(other.name()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Field {
    pub id: FieldId,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl Field {
    pub fn new(
        id: impl Into<FieldId>,
        name: impl Into<String>,
        data_type: DataType,
        nullable: bool,
    ) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            data_type,
            nullable,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Schema {
    pub id: SchemaId,
    pub fields: Vec<Field>,
}

impl Schema {
    pub fn new(id: impl Into<SchemaId>, fields: Vec<Field>) -> Result<Self> {
        let schema = Self {
            id: id.into(),
            fields,
        };
        schema.validate()?;
        Ok(schema)
    }

    pub fn validate(&self) -> Result<()> {
        let mut names = std::collections::HashSet::new();
        let mut ids = std::collections::HashSet::new();
        for field in &self.fields {
            if field.name.is_empty() {
                return Err(SparrowError::new(
                    ErrorCode::InvalidSchema,
                    "field name must not be empty",
                ));
            }
            if !names.insert(field.name.as_str()) {
                return Err(SparrowError::new(
                    ErrorCode::InvalidSchema,
                    format!("duplicate field name '{}'", field.name),
                ));
            }
            if !ids.insert(field.id) {
                return Err(SparrowError::new(
                    ErrorCode::InvalidSchema,
                    format!("duplicate field id {}", field.id),
                ));
            }
        }
        Ok(())
    }

    pub fn field_by_name(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|f| f.name == name)
    }

    pub fn field_index(&self, id: FieldId) -> Option<usize> {
        self.fields.iter().position(|f| f.id == id)
    }

    pub fn index_of_name(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|f| f.name == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_duplicate_field_names() {
        let err = Schema::new(
            1,
            vec![
                Field::new(1, "a", DataType::Int64, true),
                Field::new(2, "a", DataType::Int64, true),
            ],
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidSchema);
    }

    #[test]
    fn nested_types_are_declarable() {
        let t = DataType::Array(Box::new(DataType::Map {
            key: Box::new(DataType::Utf8),
            value: Box::new(DataType::Dynamic),
        }));
        assert!(t.is_nested());
    }
}
