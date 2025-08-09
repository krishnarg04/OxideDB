use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write, Seek, SeekFrom};
use std::sync::Mutex;
use crate::MetaEnum::MetaEnum;
use crate::RowData::RawData;

pub struct TableMetaHandler {
    file_name: String,
    table_id: HashMap<String, i64>,
    table_id_meta: HashMap<i64, Vec<MetaEnum>>,
}

pub static meta_config: Mutex<Option<TableMetaHandler>> = Mutex::new(None);

#[derive(Clone)]
enum DataTypeVsId {
    INTEGER = 1,
    FLOAT = 2,
    DOUBLE = 3,
    BIGINT = 4,
    STRING = 5,
}

impl DataTypeVsId {
    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(DataTypeVsId::INTEGER),
            2 => Some(DataTypeVsId::FLOAT),
            3 => Some(DataTypeVsId::DOUBLE),
            4 => Some(DataTypeVsId::BIGINT),
            5 => Some(DataTypeVsId::STRING),
            _ => None,
        }
    }

    fn to_meta_enum(&self, string_length: Option<i32>) -> MetaEnum {
        match self {
            DataTypeVsId::INTEGER => MetaEnum::INTEGER,
            DataTypeVsId::FLOAT => MetaEnum::FLOAT,
            DataTypeVsId::DOUBLE => MetaEnum::DOUBLE,
            DataTypeVsId::BIGINT => MetaEnum::BIGINT,
            DataTypeVsId::STRING => MetaEnum::STRING(string_length.unwrap_or(0) as i64),
        }
    }

    fn from_meta_enum(meta: &MetaEnum) -> (Self, Option<i32>) {
        match meta {
            MetaEnum::INTEGER => (DataTypeVsId::INTEGER, None),
            MetaEnum::FLOAT => (DataTypeVsId::FLOAT, None),
            MetaEnum::DOUBLE => (DataTypeVsId::DOUBLE, None),
            MetaEnum::BIGINT => (DataTypeVsId::BIGINT, None),
            MetaEnum::STRING(len) => (DataTypeVsId::STRING, Some(*len as i32)),
        }
    }
}

pub struct TableMetadata {
    pub(crate) table_id: i32,
    table_name: String,
    columns: Vec<MetaEnum>,
}

impl TableMetaHandler {
    pub fn new(file_name: String) -> Self {
        TableMetaHandler {
            file_name,
            table_id: HashMap::new(),
            table_id_meta: HashMap::new(),
        }
    }
    
    pub fn load_table_schema_meta(&mut self) -> Result<(), std::io::Error> {
        let file_path = "schema/table_meta";

        if !std::path::Path::new(file_path).exists() {
            fs::create_dir_all("schema")?;

            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .open(file_path)?;
            
            let mut writer = BufWriter::new(&mut file);

            let tables_to_create = vec![
                (1, "TableIdVsRange"),
                (2, "tableVsColumn"),
            ];

            let num_entries = tables_to_create.len() as i32;
            writer.write_all(&num_entries.to_le_bytes())?;

            for (id, name) in &tables_to_create {
                let name_bytes = name.as_bytes();
                let name_len = name_bytes.len() as i32;
                let entry_size = 4 + name_len + 4;

                writer.write_all(&entry_size.to_le_bytes())?;
                writer.write_all(&name_len.to_le_bytes())?;
                writer.write_all(name_bytes)?;
                writer.write_all(&(*id as i32).to_le_bytes())?;

                self.table_id.insert(name.to_string(), *id as i64);
            }
            writer.flush()?;
        } else {
            let mut file = OpenOptions::new().read(true).open(file_path)?;
            let mut reader = BufReader::new(file);

            let mut num_entries_bytes = [0u8; 4];
            if reader.read_exact(&mut num_entries_bytes).is_err() {
                // File is empty or corrupt, can decide to handle this case.
                return Ok(()); 
            }
            let num_entries = i32::from_le_bytes(num_entries_bytes);

            for _ in 0..num_entries {
                let mut entry_size_bytes = [0u8; 4];
                reader.read_exact(&mut entry_size_bytes)?;
                // let _entry_size = i32::from_le_bytes(entry_size_bytes);

                let mut name_len_bytes = [0u8; 4];
                reader.read_exact(&mut name_len_bytes)?;
                let name_len = i32::from_le_bytes(name_len_bytes);

                let mut name_bytes = vec![0u8; name_len as usize];
                reader.read_exact(&mut name_bytes)?;
                let table_name = String::from_utf8(name_bytes)
                    .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid UTF-8"))?;

                let mut table_id_bytes = [0u8; 4];
                reader.read_exact(&mut table_id_bytes)?;
                let table_id = i32::from_le_bytes(table_id_bytes);

                self.table_id.insert(table_name, table_id as i64);
            }
        }
        Ok(())
    }

    pub fn load_meta_file(&mut self) -> Result<Vec<TableMetadata>, std::io::Error> {

        if !std::path::Path::new(&self.file_name).exists() {

        OpenOptions::new()
            .write(true)
            .create(true)
            .open(&self.file_name)?;
        
        return Ok(Vec::new());
    }

        let mut file = OpenOptions::new()
            .read(true)
            .open(&self.file_name)?;
        
        let mut reader = BufReader::new(file);
        let mut tables = Vec::new();
        
        loop {
            let mut length_bytes = [0u8; 4];
            match reader.read_exact(&mut length_bytes) {
                Ok(_) => {},
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    break;
                },
                Err(e) => return Err(e),
            }
            
            let data_length = i32::from_le_bytes(length_bytes);
            
            let mut table_id_bytes = [0u8; 4];
            reader.read_exact(&mut table_id_bytes)?;
            let table_id = i32::from_le_bytes(table_id_bytes);
            
            let mut table_name_length_bytes = [0u8; 4];
            reader.read_exact(&mut table_name_length_bytes)?;
            let table_name_length = i32::from_le_bytes(table_name_length_bytes);
            
            let mut table_name_bytes = vec![0u8; table_name_length as usize];
            reader.read_exact(&mut table_name_bytes)?;
            let table_name = String::from_utf8_lossy(&table_name_bytes).to_string();
            
            let mut num_columns_bytes = [0u8; 4];
            reader.read_exact(&mut num_columns_bytes)?;
            let num_columns = i32::from_le_bytes(num_columns_bytes);
            
            let mut columns = Vec::new();
            
            // Read each column metadata
            for _ in 0..num_columns {
                // Read data type (1 byte)
                let mut data_type_byte = [0u8; 1];
                reader.read_exact(&mut data_type_byte)?;
                
                let data_type = DataTypeVsId::from_byte(data_type_byte[0])
                    .ok_or_else(|| std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Invalid data type ID: {}", data_type_byte[0])
                    ))?;
                
                let string_length = match data_type {
                    DataTypeVsId::STRING => {
                        let mut string_length_bytes = [0u8; 4];
                        reader.read_exact(&mut string_length_bytes)?;
                        Some(i32::from_le_bytes(string_length_bytes))
                    },
                    _ => None,
                };
                
                // Convert to MetaEnum and add to columns
                columns.push(data_type.to_meta_enum(string_length));
            }
            
            let table_metadata = TableMetadata {
                table_id,
                table_name: table_name.clone(),
                columns: columns.clone(),
            };
            
            // Store in HashMaps
            self.table_id.insert(table_name.clone(), table_id as i64);
            self.table_id_meta.insert(table_id as i64, columns);
            
            tables.push(table_metadata);
        }
        
        Ok(tables)
    }

    pub fn write_meta_file(&self, tables: &[TableMetadata]) -> Result<(), std::io::Error> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.file_name)?;
        
        let mut writer = BufWriter::new(file);
        
        for table in tables {
            // Calculate the total length for this table entry
            let mut data_length = 4 + 4 + table.table_name.len() as i32 + 4; // table_id + name_length + name + num_columns
            
            for column in &table.columns {
                data_length += 1; // data type byte
                if let MetaEnum::STRING(_) = column {
                    data_length += 4; // string length
                }
            }
            
            // Write length (4 bytes)
            writer.write_all(&data_length.to_le_bytes())?;
            
            // Write table ID (4 bytes)
            writer.write_all(&table.table_id.to_le_bytes())?;
            
            // Write table name length (4 bytes)
            let table_name_length = table.table_name.len() as i32;
            writer.write_all(&table_name_length.to_le_bytes())?;
            
            // Write table name
            writer.write_all(table.table_name.as_bytes())?;
            
            // Write number of columns (4 bytes)
            let num_columns = table.columns.len() as i32;
            writer.write_all(&num_columns.to_le_bytes())?;
            
            // Write each column metadata
            for column in &table.columns {
                let (data_type, string_length) = DataTypeVsId::from_meta_enum(column);
                
                // Write data type (1 byte)
                writer.write_all(&[data_type as u8])?;
                
                // Write string length if it's a STRING type
                if let Some(length) = string_length {
                    writer.write_all(&length.to_le_bytes())?;
                }
            }
        }
        
        writer.flush()?;
        Ok(())
    }

    pub fn create_raw_data_for_table(&self, table_name: &str, page_size: usize, header_size: usize, page_id: u64) -> Option<RawData> {
        if let Some(meta_data) = self.get_table_meta_by_name(table_name) {
            Some(RawData::new_without_array(
                table_name.to_string(),
                meta_data,
                page_size,
                header_size,
                page_id,
            ))
        } else {
            None
        }
    }

    pub fn add_table(&mut self, table_id: i32, table_name: String, columns: Vec<MetaEnum>) -> Result<(), std::io::Error> {
        self.table_id.insert(table_name.clone(), table_id as i64);
        self.table_id_meta.insert(table_id as i64, columns.clone());
        self.append_table_to_file(table_id, &table_name, &columns)?;
        
        Ok(())
    }

    fn append_table_to_file(&self, table_id: i32, table_name: &str, columns: &[MetaEnum]) -> Result<(), std::io::Error> {
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(&self.file_name)?;
        
        let mut writer = BufWriter::new(file);
        
        let mut data_length = 4 + 4 + table_name.len() as i32 + 4; 
        
        for column in columns {
            data_length += 1; // data type byte
            if let MetaEnum::STRING(_) = column {
                data_length += 4; // string length
            }
        }
        
        writer.write_all(&data_length.to_le_bytes())?;
        
        writer.write_all(&table_id.to_le_bytes())?;
        
        let table_name_length = table_name.len() as i32;
        writer.write_all(&table_name_length.to_le_bytes())?;
        
        writer.write_all(table_name.as_bytes())?;
        
        let num_columns = columns.len() as i32;
        writer.write_all(&num_columns.to_le_bytes())?;
        
        for column in columns {
            let (data_type, string_length) = DataTypeVsId::from_meta_enum(column);
            
            writer.write_all(&[data_type as u8])?;
            
            if let Some(length) = string_length {
                writer.write_all(&length.to_le_bytes())?;
            }
        }
        
        writer.flush()?;
        Ok(())
    }

    pub fn get_all_tables(&self) -> Vec<TableMetadata> {
        let mut tables = Vec::new();
        
        for (table_name, &table_id) in &self.table_id {
            if let Some(columns) = self.table_id_meta.get(&table_id) {
                tables.push(TableMetadata {
                    table_id: table_id as i32,
                    table_name: table_name.clone(),
                    columns: columns.clone(),
                });
            }
        }
        
        tables
    }
    
    pub fn get_table_id(&self, table_name: &str) -> Option<i64> {
        self.table_id.get(table_name).copied()
    }
    
    pub fn get_table_meta(&self, table_id: i64) -> Option<&Vec<MetaEnum>> {
        self.table_id_meta.get(&table_id)
    }
    
    pub fn get_table_meta_by_name(&self, table_name: &str) -> Option<&Vec<MetaEnum>> {
        if let Some(table_id) = self.get_table_id(table_name) {
            self.table_id_meta.get(&table_id)
        } else {
            None
        }
    }
}


