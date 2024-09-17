use itertools::Itertools;
use parquet::data_type::ByteArray;
use parquet::errors::ParquetError;
use parquet::file::writer::SerializedFileWriter;
use parquet::record::Field;
use std::io::Write;

#[derive(Debug)]
#[derive(Default)]
pub struct RowBuffer {
    rows: Vec<Vec<Field>>,
    current_col: usize,
    not_null: Vec<i16>,
    bools: Vec<bool>,
    i32s: Vec<i32>,
    i64s: Vec<i64>,
    f32s: Vec<f32>,
    f64s: Vec<f64>,
    strs: Vec<ByteArray>,
}


impl RowBuffer {
    pub fn begin(&mut self) {
        debug_assert_eq!(self.current_col, self.rows.len());
        self.current_col = 0;
    }

    pub fn push(&mut self, val: Field) {
        if self.rows.len() <= self.current_col {
            self.rows.resize_with(self.current_col + 1, Vec::new);
        }
        self.rows[self.current_col].push(val);
        self.current_col += 1;
    }

    pub fn record<W: Write + Send>(&mut self, writer: &mut SerializedFileWriter<W>) -> Result<usize, ParquetError> {
        debug_assert_eq!(
            self.current_col,
            self.rows.len(),
            "current {} actual {}",
            self.current_col,
            self.rows.len()
        );
        debug_assert_eq!(1, self.rows.iter().map(|c| c.len()).sorted().dedup().count());

        let size = self.len();
        if size == 0 {
            return Ok(0);
        }

        let mut row_group_writer = writer.next_row_group()?;
        let not_null = &mut self.not_null;

        for col in self.rows.iter_mut() {
            let mut column_writer = row_group_writer.next_column()?.unwrap();

            not_null.clear();
            not_null.extend(col.iter().map(|f| if matches!(f, Field::Null) { 0 } else { 1 }));

            match column_writer.untyped() {
                parquet::column::writer::ColumnWriter::BoolColumnWriter(ref mut typed_writer) => {
                    self.bools.clear();
                    for f in col.iter() {
                        match f {
                            Field::Bool(val) => self.bools.push(*val),
                            Field::Null => (),
                            _ => return Err(ParquetError::General(format!("invalid type, expected bool - {:?}", f))),
                        }
                    }
                    typed_writer.write_batch(&self.bools, Some(&not_null[..]), None)?;
                }
                parquet::column::writer::ColumnWriter::Int32ColumnWriter(ref mut typed_writer) => {
                    self.i32s.clear();
                    for f in col.iter() {
                        match f {
                            Field::Int(val) => self.i32s.push(*val),
                            Field::UInt(val) => self.i32s.push(*val as i32),
                            Field::Null => (),
                            _ => return Err(ParquetError::General(format!("invalid type, expected int32 - {:?}", f))),
                        }
                    }
                    typed_writer.write_batch(&self.i32s, Some(&not_null[..]), None)?;
                }
                parquet::column::writer::ColumnWriter::Int64ColumnWriter(ref mut typed_writer) => {
                    self.i64s.clear();
                    for f in col.iter() {
                        match f {
                            Field::Long(val) => self.i64s.push(*val),
                            Field::ULong(val) => self.i64s.push(*val as i64),
                            Field::Null => (),
                            _ => return Err(ParquetError::General(format!("invalid type, expected int64 - {:?}", f))),
                        }
                    }
                    typed_writer.write_batch(&self.i64s, Some(&not_null[..]), None)?;
                }
                parquet::column::writer::ColumnWriter::FloatColumnWriter(ref mut typed_writer) => {
                    self.f32s.clear();
                    for f in col.iter() {
                        match f {
                            Field::Float(val) => self.f32s.push(*val),
                            Field::Null => (),
                            _ => return Err(ParquetError::General(format!("invalid type, expected float - {:?}", f))),
                        }
                    }
                    typed_writer.write_batch(&self.f32s, Some(&not_null[..]), None)?;
                }
                parquet::column::writer::ColumnWriter::DoubleColumnWriter(ref mut typed_writer) => {
                    self.f64s.clear();
                    for f in col.iter() {
                        match f {
                            Field::Double(val) => self.f64s.push(*val),
                            Field::Null => (),
                            _ => {
                                return Err(ParquetError::General(format!(
                                    "invalid type, expected double - {:?}",
                                    f
                                )))
                            }
                        }
                    }
                    typed_writer.write_batch(&self.f64s, Some(&not_null[..]), None)?;
                }
                parquet::column::writer::ColumnWriter::ByteArrayColumnWriter(ref mut typed_writer) => {
                    self.strs.clear();
                    for f in col.iter() {
                        match f {
                            Field::Str(ref val) => self.strs.push(ByteArray::from(val.as_str())),
                            Field::Null => (),
                            _ => {
                                return Err(ParquetError::General(format!(
                                    "invalid type, expected byte array - {:?}",
                                    f
                                )))
                            }
                        }
                    }
                    typed_writer.write_batch(&self.strs, Some(&not_null[..]), None)?;
                }
                _ => return Err(ParquetError::General("unsupported column writer type".to_string())),
            }
            column_writer.close()?;
            col.clear();
        }
        row_group_writer.close()?;
        Ok(size)
    }

    pub fn len(&self) -> usize {
        self.rows.first().map(|c| c.len()).unwrap_or_default()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
