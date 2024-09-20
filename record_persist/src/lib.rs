pub mod config;
pub mod error;
pub mod row;
pub mod writer;

use crate::row::RowBuffer;

use chrono::{DateTime, TimeZone};
use compact_str::CompactString;
use parquet::basic::Type as PhysicalType;
use parquet::basic::{LogicalType, Repetition, TimeUnit};
use parquet::errors::ParquetError;
use parquet::format::NanoSeconds;
use parquet::record::Field;
use parquet::schema::types::{Type, TypePtr};
use rust_decimal::prelude::ToPrimitive;
use std::any::type_name;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::time::Duration;

pub trait Persistable {
    fn schema(
        fields: &mut Vec<TypePtr>,
        prefix: Option<&str>,
        repetition_override: Option<Repetition>,
        logical_type: Option<LogicalType>,
    );

    fn append(&self, rows: &mut RowBuffer) -> Result<(), ParquetError>;

    fn field_count() -> usize
    where
        Self: Sized,
    {
        static FIELD_COUNT: LazyLock<Mutex<HashMap<String, usize>>> = LazyLock::new(|| Mutex::new(Default::default()));

        let mut map = FIELD_COUNT.lock().unwrap();

        let name = type_name::<Self>();
        let count = match map.get(name) {
            None => {
                let mut fields = Vec::new();
                Self::schema(&mut fields, Some("any"), None, None);
                let count = fields.len();
                map.insert(name.to_string(), count);
                count
            }
            Some(count) => *count,
        };
        count
    }
}

impl Persistable for String {
    fn schema(
        fields: &mut Vec<TypePtr>,
        prefix: Option<&str>,
        repetition_override: Option<Repetition>,
        _logical_type: Option<LogicalType>,
    ) {
        let prefix = prefix.expect("name must be set");
        fields.push(
            Type::primitive_type_builder(prefix, PhysicalType::BYTE_ARRAY)
                .with_repetition(repetition_override.unwrap_or(Repetition::REQUIRED))
                .with_logical_type(Some(LogicalType::String))
                .build()
                .unwrap()
                .into(),
        );
    }

    #[inline]
    fn append(&self, row: &mut RowBuffer) -> Result<(), ParquetError> {
        row.push(parquet::record::Field::Str(self.clone()));
        Ok(())
    }
}

impl<T: Persistable> Persistable for Option<T> {
    fn schema(
        fields: &mut Vec<TypePtr>,
        prefix: Option<&str>,
        _repetition_override: Option<Repetition>,
        logical_type: Option<LogicalType>,
    ) {
        T::schema(fields, prefix, Some(Repetition::OPTIONAL), logical_type);
    }

    #[inline]
    fn append(&self, row: &mut RowBuffer) -> Result<(), ParquetError> {
        if let Some(ref value) = *self {
            T::append(value, row)?;
        } else {
            for _ in 0..T::field_count() {
                row.push(Field::Null);
            }
        }
        Ok(())
    }
}

impl Persistable for &str {
    fn schema(
        fields: &mut Vec<TypePtr>,
        prefix: Option<&str>,
        repetition_override: Option<Repetition>,
        _logical_type: Option<LogicalType>,
    ) {
        let prefix = prefix.expect("name must be set");
        fields.push(
            Type::primitive_type_builder(prefix, PhysicalType::BYTE_ARRAY)
                .with_repetition(repetition_override.unwrap_or(Repetition::REQUIRED))
                .with_logical_type(Some(LogicalType::String))
                .build()
                .unwrap()
                .into(),
        );
    }

    #[inline]
    fn append(&self, row: &mut RowBuffer) -> Result<(), ParquetError> {
        row.push(parquet::record::Field::Str(self.to_string()));
        Ok(())
    }
}

impl<T: Persistable + Debug> Persistable for Vec<T> {
    fn schema(
        fields: &mut Vec<TypePtr>,
        prefix: Option<&str>,
        repetition_override: Option<Repetition>,
        _logical_type: Option<LogicalType>,
    ) {
        let prefix = prefix.expect("name must be set");
        fields.push(
            Type::primitive_type_builder(prefix, PhysicalType::BYTE_ARRAY)
                .with_repetition(repetition_override.unwrap_or(Repetition::REQUIRED))
                .with_logical_type(Some(LogicalType::String))
                .build()
                .unwrap()
                .into(),
        );
    }

    #[inline]
    fn append(&self, rows: &mut RowBuffer) -> Result<(), ParquetError> {
        rows.push(Field::Str(format!("{:?}", self)));
        Ok(())
    }
}

impl<T: Persistable + Debug> Persistable for HashSet<T> {
    fn schema(
        fields: &mut Vec<TypePtr>,
        prefix: Option<&str>,
        repetition_override: Option<Repetition>,
        _logical_type: Option<LogicalType>,
    ) {
        let prefix = prefix.expect("name must be set");
        fields.push(
            Type::primitive_type_builder(prefix, PhysicalType::BYTE_ARRAY)
                .with_repetition(repetition_override.unwrap_or(Repetition::REQUIRED))
                .with_logical_type(Some(LogicalType::String))
                .build()
                .unwrap()
                .into(),
        );
    }

    #[inline]
    fn append(&self, rows: &mut RowBuffer) -> Result<(), ParquetError> {
        rows.push(Field::Str(format!("{:?}", self)));
        Ok(())
    }
}

impl<K: Persistable + Debug, V: Persistable + Debug> Persistable for HashMap<K, V> {
    fn schema(
        fields: &mut Vec<TypePtr>,
        prefix: Option<&str>,
        repetition_override: Option<Repetition>,
        _logical_type: Option<LogicalType>,
    ) {
        let prefix = prefix.expect("name must be set");
        fields.push(
            Type::primitive_type_builder(prefix, PhysicalType::BYTE_ARRAY)
                .with_repetition(repetition_override.unwrap_or(Repetition::REQUIRED))
                .with_logical_type(Some(LogicalType::String))
                .build()
                .unwrap()
                .into(),
        );
    }

    #[inline]
    fn append(&self, rows: &mut RowBuffer) -> Result<(), ParquetError> {
        rows.push(Field::Str(format!("{:?}", self)));
        Ok(())
    }
}

impl<X: Persistable, Y: Persistable> Persistable for (X, Y) {
    fn schema(
        fields: &mut Vec<TypePtr>,
        prefix: Option<&str>,
        repetition_override: Option<Repetition>,
        logical_type: Option<LogicalType>,
    ) {
        match prefix {
            None => {
                X::schema(fields, prefix, repetition_override, logical_type.clone());
                Y::schema(fields, prefix, repetition_override, logical_type.clone());
            }
            Some(prefix) => {
                X::schema(
                    fields,
                    Some(&format!("{}_0", prefix)),
                    repetition_override,
                    logical_type.clone(),
                );
                Y::schema(
                    fields,
                    Some(&format!("{}_1", prefix)),
                    repetition_override,
                    logical_type.clone(),
                );
            }
        }
    }

    #[inline]
    fn append(&self, row: &mut RowBuffer) -> Result<(), ParquetError> {
        self.0.append(row)?;
        self.1.append(row)?;
        Ok(())
    }
}

macro_rules! define_schema {
    ($type:ty, $physical_type:expr) => {
        fn schema(
            fields: &mut Vec<TypePtr>,
            prefix: Option<&str>,
            repetition_override: Option<Repetition>,
            logical_type: Option<LogicalType>,
        ) {
            let prefix = prefix.expect("name must be set");
            fields.push(
                Type::primitive_type_builder(prefix, $physical_type)
                    .with_repetition(repetition_override.unwrap_or(Repetition::REQUIRED))
                    .with_logical_type(logical_type)
                    .build()
                    .unwrap()
                    .into(),
            );
        }
    };
}

macro_rules! build_primitive {
    ($type:ty, $physical_type:expr, $field_type:expr, $convert:ty) => {
        impl Persistable for $type {
            define_schema!($type, $physical_type);

            #[inline]
            fn append(&self, row: &mut RowBuffer) -> Result<(), ParquetError> {
                row.push($field_type(*self as $convert));
                Ok(())
            }
        }
    };
    ($type:ty, $physical_type:expr, $field_type:expr) => {
        impl Persistable for $type {
            define_schema!($type, $physical_type);

            #[inline]
            fn append(&self, row: &mut RowBuffer) -> Result<(), ParquetError> {
                row.push($field_type(*self));
                Ok(())
            }
        }
    };
}

build_primitive!(f64, PhysicalType::DOUBLE, parquet::record::Field::Double);
build_primitive!(f32, PhysicalType::FLOAT, parquet::record::Field::Float);
build_primitive!(u64, PhysicalType::INT64, parquet::record::Field::ULong);
build_primitive!(i64, PhysicalType::INT64, parquet::record::Field::Long);
build_primitive!(u32, PhysicalType::INT32, parquet::record::Field::UInt);
build_primitive!(i32, PhysicalType::INT32, parquet::record::Field::Int);
build_primitive!(bool, PhysicalType::BOOLEAN, parquet::record::Field::Bool);
build_primitive!(usize, PhysicalType::INT64, parquet::record::Field::ULong, u64);
build_primitive!(isize, PhysicalType::INT64, parquet::record::Field::Long, i64);
build_primitive!(u16, PhysicalType::INT32, parquet::record::Field::UInt, u32);
build_primitive!(i16, PhysicalType::INT32, parquet::record::Field::Int, i32);

impl<Tz: TimeZone> Persistable for DateTime<Tz> {
    fn schema(
        fields: &mut Vec<TypePtr>,
        prefix: Option<&str>,
        repetition_override: Option<Repetition>,
        _logical_type: Option<LogicalType>,
    ) {
        let prefix = prefix.expect("name must be set");
        fields.push(
            Type::primitive_type_builder(prefix, PhysicalType::INT64)
                .with_repetition(repetition_override.unwrap_or(Repetition::REQUIRED))
                .with_logical_type(Some(LogicalType::Timestamp {
                    is_adjusted_to_u_t_c: true,
                    unit: TimeUnit::NANOS(NanoSeconds::new()),
                }))
                .build()
                .unwrap()
                .into(),
        );
    }

    #[inline]
    fn append(&self, row: &mut RowBuffer) -> Result<(), ParquetError> {
        row.push(parquet::record::Field::ULong(
            self.timestamp_nanos_opt().unwrap_or_default() as u64,
        ));
        Ok(())
    }
}

impl Persistable for Duration {
    fn schema(
        fields: &mut Vec<TypePtr>,
        prefix: Option<&str>,
        repetition_override: Option<Repetition>,
        logical_type: Option<LogicalType>,
    ) {
        u64::schema(
            fields,
            prefix.map(|name| format!("{}_ns", name)).as_deref(),
            repetition_override,
            logical_type,
        );
    }

    #[inline]
    fn append(&self, row: &mut RowBuffer) -> Result<(), ParquetError> {
        let ns = self.as_nanos() as u64;
        ns.append(row)?;
        Ok(())
    }
}

macro_rules! impl_persistable_for_arrays {
    ($($len:expr),*) => {
        $(
            impl<T: Persistable> Persistable for [T; $len] {
                fn schema(fields: &mut Vec<parquet::schema::types::TypePtr>, prefix: Option<&str>, repetition_override: Option<parquet::basic::Repetition>, logical_type: Option<LogicalType>) {
                    for i in 0..$len {
                        let name = match prefix {
                            Some(p) => format!("{}_{}", p, i),
                            None => i.to_string(),
                        };
                        T::schema(fields, Some(&name), repetition_override, logical_type.clone());
                    }
                }

                #[inline]
                fn append(&self, row: &mut RowBuffer) -> Result<(), parquet::errors::ParquetError> {
                    for item in self.iter() {
                        item.append(row)?;
                    }
                    Ok(())
                }
            }
        )*
    }
}

impl_persistable_for_arrays!(
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
    32
);

impl Persistable for CompactString {
    fn schema(
        fields: &mut Vec<TypePtr>,
        prefix: Option<&str>,
        repetition_override: Option<Repetition>,
        _logical_type: Option<LogicalType>,
    ) {
        let prefix = prefix.expect("name must be set");
        fields.push(
            Type::primitive_type_builder(prefix, PhysicalType::BYTE_ARRAY)
                .with_repetition(repetition_override.unwrap_or(Repetition::REQUIRED))
                .with_logical_type(Some(LogicalType::String))
                .build()
                .unwrap()
                .into(),
        );
    }

    #[inline]
    fn append(&self, row: &mut RowBuffer) -> Result<(), ParquetError> {
        row.push(parquet::record::Field::Str(self.to_string()));
        Ok(())
    }
}

impl Persistable for rust_decimal::Decimal {
    fn schema(
        fields: &mut Vec<TypePtr>,
        prefix: Option<&str>,
        repetition_override: Option<Repetition>,
        _logical_type: Option<LogicalType>,
    ) {
        // PhysicalType::DOUBLE, parquet::record::Field::Double);
        let prefix = prefix.expect("name must be set");
        fields.push(
            Type::primitive_type_builder(prefix, PhysicalType::DOUBLE)
                .with_repetition(repetition_override.unwrap_or(Repetition::REQUIRED))
                .with_logical_type(Some(LogicalType::String))
                .build()
                .unwrap()
                .into(),
        );
    }

    #[inline]
    fn append(&self, row: &mut RowBuffer) -> Result<(), ParquetError> {
        row.push(parquet::record::Field::Double(self.to_f64().unwrap()));
        Ok(())
    }
}
