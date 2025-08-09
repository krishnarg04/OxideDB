use std::collections::HashMap;
use crate::MetaEnum::MetaEnum;
use crate::TableMetaHandler::{meta_config, TableMetaHandler};
use crate::BPlusTree::{BPlusTree, Key, data};
use crate::FileWriter::File_Handler;
use crate::RowData::RawData;

pub struct TableColumn {
    pub column_name: String,
    pub column_type: MetaEnum,
    pub is_primary: bool,
}

impl TableColumn {
    pub fn new(column_name: String, column_type: MetaEnum, is_primary: bool) -> Self {
        TableColumn {
            column_name,
            column_type,
            is_primary,
        }
    }
}

pub struct TableCreationHandler {
    table_id_vs_range_btree: BPlusTree,
    table_vs_column_btree: BPlusTree,
    file_handler: File_Handler,
}

impl TableCreationHandler {
    pub fn new() -> Self {
        TableCreationHandler {
            table_id_vs_range_btree: BPlusTree::new(),
            table_vs_column_btree: BPlusTree::new(),
            file_handler: File_Handler::new("table_metadata".to_string()),
        }
    }

    pub fn create_table(
        &mut self,
        table_name: String,
        columns: Vec<TableColumn>,
    ) -> Result<i32, String> {
        let table_id = self.get_next_table_id()?;
        
        self.add_table_meta(table_id, &table_name, &columns)?;
        
        self.add_table_columns_to_btree(table_id, &columns)?;
        
        self.update_table_id_range(table_id)?;
        
        println!("Table '{}' created successfully with ID: {}", table_name, table_id);
        Ok(table_id)
    }
    fn get_next_table_id(&self) -> Result<i32, String> {
        
        let guard = meta_config.lock().map_err(|_| "Failed to lock meta_config")?;
        let config = guard.as_ref().ok_or("Meta config not initialized")?;
   
        let all_tables = config.get_all_tables();
        let max_id = all_tables.iter()
            .map(|table| table.table_id)
            .max()
            .unwrap_or(2); 
        
        Ok(max_id + 1)
    }

    fn add_table_meta(
        &self,
        table_id: i32,
        table_name: &str,
        columns: &[TableColumn],
    ) -> Result<(), String> {
        let meta_columns: Vec<MetaEnum> = columns.iter()
            .map(|col| col.column_type.clone())
            .collect();

        let mut guard = meta_config.lock().map_err(|_| "Failed to lock meta_config")?;
        let config = guard.as_mut().ok_or("Meta config not initialized")?;
        
        config.add_table(table_id, table_name.to_string(), meta_columns)
            .map_err(|e| format!("Failed to add table to meta: {}", e))?;
        
        Ok(())
    }

    fn add_table_columns_to_btree(
        &mut self,
        table_id: i32,
        columns: &[TableColumn],
    ) -> Result<(), String> {
        
        for (column_index, column) in columns.iter().enumerate() {
            let btree_key = table_id * 1000 + column_index as i32;

            let column_data = self.serialize_column_data(table_id, column)?;

            let (page_id, offset) = self.write_column_data_to_file(&column_data)?;
            
            let data_ptr = Box::new(data::new(page_id, offset));
            let key_entry = Box::new(Key::new(btree_key, Some(data_ptr)));
            
            self.table_vs_column_btree.insert(Some(key_entry));
        }
        
        Ok(())
    }

    fn serialize_column_data(&self, table_id: i32, column: &TableColumn) -> Result<Vec<u8>, String> {
        let mut data = Vec::new();
        
        data.extend_from_slice(&table_id.to_le_bytes());
        
        let name_bytes = column.column_name.as_bytes();
        data.extend_from_slice(&(name_bytes.len() as i32).to_le_bytes());
        data.extend_from_slice(name_bytes);
        
        let type_data = self.serialize_meta_enum(&column.column_type)?;
        data.extend_from_slice(&type_data);
        
        data.push(if column.is_primary { 1 } else { 0 });
        
        Ok(data)
    }

    fn serialize_meta_enum(&self, meta_enum: &MetaEnum) -> Result<Vec<u8>, String> {
        let mut data = Vec::new();
        
        match meta_enum {
            MetaEnum::INTEGER => {
                data.push(1); 
            },
            MetaEnum::FLOAT => {
                data.push(2);
            },
            MetaEnum::DOUBLE => {
                data.push(3); 
            },
            MetaEnum::BIGINT => {
                data.push(4); 
            },
            MetaEnum::STRING(length) => {
                data.push(5); 
                data.extend_from_slice(&(*length as i32).to_le_bytes());
            },
        }
        
        Ok(data)
    }

    fn write_column_data_to_file(&self, column_data: &[u8]) -> Result<(i64, i32), String> {
        let column_meta_schema = vec![
            MetaEnum::INTEGER, 
            MetaEnum::STRING(256), 
            MetaEnum::INTEGER, 
            MetaEnum::INTEGER, 
            MetaEnum::INTEGER, 
        ];
        
        let page_id = 0i64;
        
        
        let mut raw_data = RawData::new_without_array(
            "tableVsColumn".to_string(),
            &column_meta_schema,
            4096, 
            64,   
            page_id as u64,
        );
        
        
        raw_data.add_new_row(column_data);
        
        
        self.file_handler.write_to_file(&raw_data);
        
        
        Ok((page_id, 0))
    }

    
    fn update_table_id_range(&mut self, table_id: i32) -> Result<(), String> {
        
        
        
        let data_ptr = Box::new(data::new(0, table_id)); 
        let key_entry = Box::new(Key::new(table_id, Some(data_ptr)));
        
        self.table_id_vs_range_btree.insert(Some(key_entry));
        
        Ok(())
    }

    
    pub fn get_table_columns(&self, table_id: i32) -> Result<Vec<TableColumn>, String> {

        Ok(Vec::new())
    }

    
    fn validate_table_creation(
        table_name: &str,
        columns: &[TableColumn],
    ) -> Result<(), String> {
        if table_name.is_empty() {
            return Err("Table name cannot be empty".to_string());
        }
        
        if columns.is_empty() {
            return Err("Table must have at least one column".to_string());
        }
        
        
        let mut column_names = std::collections::HashSet::new();
        for column in columns {
            if !column_names.insert(&column.column_name) {
                return Err(format!("Duplicate column name: {}", column.column_name));
            }
        }
        
        
        let primary_count = columns.iter().filter(|col| col.is_primary).count();
        if primary_count > 1 {
            return Err("Table cannot have multiple primary key columns".to_string());
        }
        
        Ok(())
    }

    
    pub fn create_table_with_validation(
        &mut self,
        table_name: String,
        columns: Vec<TableColumn>,
    ) -> Result<i32, String> {
        
        Self::validate_table_creation(&table_name, &columns)?;
        
        
        let guard = meta_config.lock().map_err(|_| "Failed to lock meta_config")?;
        let config = guard.as_ref().ok_or("Meta config not initialized")?;
        
        if config.get_table_id(&table_name).is_some() {
            return Err(format!("Table '{}' already exists", table_name));
        }
        drop(guard); 
        
        
        self.create_table(table_name, columns)
    }
}


pub fn create_table_handler() -> TableCreationHandler {
    TableCreationHandler::new()
}


pub fn example_create_table() -> Result<(), String> {
    let mut handler = TableCreationHandler::new();
    
    
    let columns = vec![
        TableColumn::new("id".to_string(), MetaEnum::INTEGER, true),
        TableColumn::new("name".to_string(), MetaEnum::STRING(100), false),
        TableColumn::new("email".to_string(), MetaEnum::STRING(255), false),
        TableColumn::new("age".to_string(), MetaEnum::INTEGER, false),
        TableColumn::new("salary".to_string(), MetaEnum::DOUBLE, false),
    ];
    
    
    let table_id = handler.create_table_with_validation("users".to_string(), columns)?;
    
    println!("Created table 'users' with ID: {}", table_id);
    Ok(())
}