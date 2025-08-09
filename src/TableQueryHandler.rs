use std::collections::HashMap;
use crate::MetaEnum::{MetaEnum, DataArray, row_array};
use crate::RowData::RawData;
use crate::BPlusTree::{BPlusTree, Key, data};
use crate::FileWriter::File_Handler;
use crate::TableMetaHandler::{meta_config, TableMetaHandler};
use crate::BTreePersistence::{BTreePersistence, save_btree_manually};

pub struct TableQueryHandler {
    table_indexes: HashMap<String, BPlusTree>,
    table_file_handlers: HashMap<String, File_Handler>,
    table_page_info: HashMap<String, (u64, i32)>, 
}

impl TableQueryHandler {
    pub fn new() -> Self {
        TableQueryHandler {
            table_indexes: HashMap::new(),
            table_file_handlers: HashMap::new(),
            table_page_info: HashMap::new(),
        }
    }

    fn get_file_handler(&mut self, table_name: &str) -> &File_Handler {
        self.table_file_handlers
            .entry(table_name.to_string())
            .or_insert_with(|| File_Handler::new(table_name.to_string()))
    }

    pub fn insert(
        &mut self,
        table_name: String,
        primary_key: i32,
        row_data: row_array,
    ) -> Result<(), String> {
        let table_meta = self.get_table_metadata(&table_name)?;
        
        self.validate_row_data(&table_meta, &row_data)?;
        
        if !self.table_indexes.contains_key(&table_name) {
            self.table_indexes.insert(table_name.clone(), BPlusTree::new());
        }

        let (current_page_id, _current_row_count) = self.get_current_page_info(&table_name);
        let mut raw_data = if std::path::Path::new(&format!("{}.dat", table_name)).exists() {

            match std::panic::catch_unwind(|| {
                File_Handler::read_from_file(
                    table_name.clone(),
                    current_page_id,
                    4096,
                )
            }) {
                Ok(data) => data,
                Err(_) => {
                    RawData::new_without_array(
                        table_name.clone(),
                        &table_meta,
                        4096, 
                        64, 
                        current_page_id,
                    )
                }
            }
        } else {
            // Create new page
            RawData::new_without_array(
                table_name.clone(),
                &table_meta,
                4096, 
                64,   
                current_page_id,
            )
        };
        
        let current_row_count = self.get_current_row_count(&raw_data)?;
        
        let row_bytes = row_data.get_data_as_bytes();
        raw_data.add_new_row(&row_bytes);
        
        let row_offset = current_row_count;
        
        let file_handler = self.get_file_handler(&table_name);
        file_handler.write_to_file(&raw_data);
        
        self.table_page_info.insert(table_name.clone(), (current_page_id, current_row_count + 1));
        
        let data_ptr = Box::new(data::new(current_page_id as i64, row_offset));
        let key_entry = Box::new(Key::new(primary_key, Some(data_ptr)));
        
        let btree = self.table_indexes.get_mut(&table_name).unwrap();
        btree.insert(Some(key_entry));
        
        println!("Inserted row with primary key {} into table '{}' at page {} offset {}", 
                 primary_key, table_name, current_page_id, row_offset);
        Ok(())
    }


    pub fn select(
        &self,
        table_name: String,
        primary_key: i32,
    ) -> Result<Option<String>, String> {
        if !self.table_indexes.contains_key(&table_name) {
            return Err(format!("Table '{}' not found or has no data", table_name));
        }
        
        let btree = self.table_indexes.get(&table_name).unwrap();
        let search_result = btree.search(primary_key);
        
        match search_result {
            Some(data_ref) => {
                let page_id = data_ref.page_id as u64;
                let offset = data_ref.offset;
                
                let raw_data = File_Handler::read_from_file(
                    table_name.clone(),
                    page_id,
                    4096, 
                );
                
                let row_string = raw_data.data_as_str(offset as usize);
                Ok(Some(row_string))
            },
            None => Ok(None),
        }
    }



    fn get_table_metadata(&self, table_name: &str) -> Result<Vec<MetaEnum>, String> {
        let guard = meta_config.lock().map_err(|_| "Failed to lock meta_config")?;
        let config = guard.as_ref().ok_or("Meta config not initialized")?;
        
        config.get_table_meta_by_name(table_name)
            .cloned()
            .ok_or_else(|| format!("Table '{}' not found", table_name))
    }

    fn validate_row_data(&self, table_meta: &[MetaEnum], row_data: &row_array) -> Result<(), String> {
        if table_meta.len() != row_data.data.len() {
            return Err(format!(
                "Column count mismatch: expected {}, got {}",
                table_meta.len(),
                row_data.data.len()
            ));
        }

        for (i, (expected_type, actual_data)) in table_meta.iter().zip(row_data.data.iter()).enumerate() {
            if !self.types_match(expected_type, actual_data) {
                return Err(format!(
                    "Type mismatch at column {}: expected {}, got {}",
                    i, 
                    self.type_name(expected_type),
                    self.data_type_name(actual_data)
                ));
            }
        }

        Ok(())
    }

    fn types_match(&self, meta_type: &MetaEnum, data_type: &DataArray) -> bool {
        match (meta_type, data_type) {
            (MetaEnum::INTEGER, DataArray::INTEGER(_)) => true,
            (MetaEnum::FLOAT, DataArray::FLOAT(_)) => true,
            (MetaEnum::DOUBLE, DataArray::DOUBLE(_)) => true,
            (MetaEnum::BIGINT, DataArray::BIGINT(_)) => true,
            (MetaEnum::STRING(_), DataArray::STRING(_, _)) => true,
            _ => false,
        }
    }

    fn type_name(&self, meta_type: &MetaEnum) -> String {
        match meta_type {
            MetaEnum::INTEGER => "INTEGER".to_string(),
            MetaEnum::FLOAT => "FLOAT".to_string(),
            MetaEnum::DOUBLE => "DOUBLE".to_string(),
            MetaEnum::BIGINT => "BIGINT".to_string(),
            MetaEnum::STRING(len) => format!("STRING({})", len),
        }
    }

    fn data_type_name(&self, data_type: &DataArray) -> String {
        match data_type {
            DataArray::INTEGER(_) => "INTEGER".to_string(),
            DataArray::FLOAT(_) => "FLOAT".to_string(),
            DataArray::DOUBLE(_) => "DOUBLE".to_string(),
            DataArray::BIGINT(_) => "BIGINT".to_string(),
            DataArray::STRING(_, len) => format!("STRING({})", len),
        }
    }

    fn get_current_page_info(&mut self, table_name: &str) -> (u64, i32) {

        *self.table_page_info.entry(table_name.to_string()).or_insert((0, 0))
    }

    fn get_current_row_count(&self, raw_data: &RawData) -> Result<i32, String> {
        const OFFSET_SIZE: usize = std::mem::size_of::<i32>();
        
        let row_count_bytes: [u8; OFFSET_SIZE] = raw_data.data[raw_data.header_size..raw_data.header_size + OFFSET_SIZE]
            .try_into()
            .map_err(|_| "Failed to read row count")?;
        
        let row_count = i32::from_le_bytes(row_count_bytes);
        Ok(row_count)
    }

    fn get_row_count_after_insert(&self, raw_data: &RawData) -> Result<i32, String> {
        self.get_current_row_count(raw_data)
    }

    pub fn create_row(
        &self,
        table_name: &str,
        values: Vec<DataArray>,
    ) -> Result<row_array, String> {
        let table_meta = self.get_table_metadata(table_name)?;
        
        let mut row = row_array::new();
        row.add_meta_array(&table_meta);
        row.add_array(values);
        
        Ok(row)
    }

    pub fn batch_insert(
        &mut self,
        table_name: String,
        rows: Vec<(i32, row_array)>, 
    ) -> Result<(), String> {
        for (primary_key, row_data) in rows {
            self.insert(table_name.clone(), primary_key, row_data)?;
        }
        Ok(())
    }

    pub fn get_available_tables(&self) -> Vec<String> {
        self.table_indexes.keys().cloned().collect()
    }

    pub fn key_exists(&self, table_name: &str, primary_key: i32) -> bool {
        if let Some(btree) = self.table_indexes.get(table_name) {
            btree.search(primary_key).is_some()
        } else {
            false
        }
    }

    pub fn load_existing_btrees(&mut self) {
        let table_names = self.discover_existing_tables();
        
        for table_name in table_names {
            match BTreePersistence::load_btree(&table_name) {
                Ok(btree) => {
                    self.get_file_handler(&table_name);
                    self.table_indexes.insert(table_name.clone(), btree);
                    
                    self.restore_page_info(&table_name);
                    
                    println!("Loaded table '{}' with B+Tree", table_name);
                },
                Err(e) => {
                    eprintln!("Failed to load BTree for table {}: {}", table_name, e);
                }
            }
        }
    }

    fn restore_page_info(&mut self, table_name: &str) {
        let data_file = format!("{}.dat", table_name);
        
        if !std::path::Path::new(&data_file).exists() {
            return;
        }
        
        match std::fs::metadata(&data_file) {
            Ok(metadata) => {
                let file_size = metadata.len();
                let page_size = 4096u64;
                
                if file_size == 0 {
                    self.table_page_info.insert(table_name.to_string(), (0, 0));
                    return;
                }
                
                let num_pages = (file_size + page_size - 1) / page_size; // Round up
                
                let last_page_id = if num_pages > 0 { num_pages - 1 } else { 0 };
                
                let table_name_clone = table_name.to_string();
                let row_count = std::panic::catch_unwind(move || {
                    let raw_data = File_Handler::read_from_file(table_name_clone, last_page_id, 4096);
                    const OFFSET_SIZE: usize = std::mem::size_of::<i32>();
                    if raw_data.data.len() >= raw_data.header_size + OFFSET_SIZE {
                        let row_count_bytes: [u8; OFFSET_SIZE] = raw_data.data[raw_data.header_size..raw_data.header_size + OFFSET_SIZE]
                            .try_into()
                            .unwrap_or([0; OFFSET_SIZE]);
                        i32::from_le_bytes(row_count_bytes)
                    } else {
                        0
                    }
                }).unwrap_or(0);
                
                self.table_page_info.insert(table_name.to_string(), (last_page_id, row_count));
                
                println!("Restored page info for '{}': page={}, row_count={}, file_size={}", 
                        table_name, last_page_id, row_count, file_size);
            },
            Err(e) => {
                println!("Failed to get file metadata for {}: {}", table_name, e);
                self.table_page_info.insert(table_name.to_string(), (0, 0));
            }
        }
    }

    fn discover_existing_tables(&self) -> Vec<String> {
        let mut table_names = Vec::new();
        
        println!("Discovering existing tables...");
        
        if let Ok(entries) = std::fs::read_dir(".") {
            for entry in entries {
                if let Ok(entry) = entry {
                    let path = entry.path();
                    if let Some(extension) = path.extension() {
                        if extension == "dat" {
                            if let Some(stem) = path.file_stem() {
                                if let Some(table_name) = stem.to_str() {
                                    // Filter out internal system files
                                    if !self.is_system_file(table_name) {
                                        println!("Found table file: {}.dat", table_name);
                                        table_names.push(table_name.to_string());
                                    } else {
                                        println!("Skipping system file: {}.dat", table_name);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        println!("Discovered {} tables: {:?}", table_names.len(), table_names);
        table_names
    }

    fn is_system_file(&self, table_name: &str) -> bool {
        match table_name {
            "table_metadata" => true,
            "meta_config" => true,
            _ => false,
        }
    }

    pub fn save_btrees(&self) -> Result<(), String> {
        BTreePersistence::save_all_btrees(&self.table_indexes)
    }
}






