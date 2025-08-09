use std::collections::HashMap;
use std::sync::Mutex;
use crate::UniversalBPlusTree::{BPlusTree, IntBPlusTree, StringBPlusTree, BigIntBPlusTree, DoubleBPlusTree};
use crate::UniversalKey::{Key, data, IntKey, StringKey, BigIntKey, DoubleKey};
use crate::MetaEnum::MetaEnum;


pub enum TableBTree {
    IntTree(IntBPlusTree),
    StringTree(StringBPlusTree),
    BigIntTree(BigIntBPlusTree),
    DoubleTree(DoubleBPlusTree),
}

impl TableBTree {
    
    pub fn new(key_type: &MetaEnum) -> Self {
        match key_type {
            MetaEnum::INTEGER => TableBTree::IntTree(BPlusTree::new()),
            MetaEnum::STRING(_) => TableBTree::StringTree(BPlusTree::new()),
            MetaEnum::BIGINT => TableBTree::BigIntTree(BPlusTree::new()),
            MetaEnum::DOUBLE | MetaEnum::FLOAT => TableBTree::DoubleTree(BPlusTree::new()),
        }
    }

    
    pub fn insert(&mut self, key_value: TableKey, page_id: i64, offset: i32) -> Result<(), String> {
        let data_ptr = Box::new(data::new(page_id, offset));
        
        match (self, key_value) {
            (TableBTree::IntTree(tree), TableKey::Int(val)) => {
                let key = Box::new(IntKey::new(val, Some(data_ptr)));
                tree.insert(Some(key));
                Ok(())
            },
            (TableBTree::StringTree(tree), TableKey::String(val)) => {
                let key = Box::new(StringKey::new(val, Some(data_ptr)));
                tree.insert(Some(key));
                Ok(())
            },
            (TableBTree::BigIntTree(tree), TableKey::BigInt(val)) => {
                let key = Box::new(BigIntKey::new(val, Some(data_ptr)));
                tree.insert(Some(key));
                Ok(())
            },
            (TableBTree::DoubleTree(tree), TableKey::Double(val)) => {
                let key = Box::new(DoubleKey::new(val, Some(data_ptr)));
                tree.insert(Some(key));
                Ok(())
            },
            _ => Err("Key type mismatch with B+Tree type".to_string()),
        }
    }

    
    pub fn search(&self, key_value: &TableKey) -> Option<Box<data>> {
        match (self, key_value) {
            (TableBTree::IntTree(tree), TableKey::Int(val)) => {
                tree.search(val)
            },
            (TableBTree::StringTree(tree), TableKey::String(val)) => {
                tree.search(val)
            },
            (TableBTree::BigIntTree(tree), TableKey::BigInt(val)) => {
                tree.search(val)
            },
            (TableBTree::DoubleTree(tree), TableKey::Double(val)) => {
                tree.search(val)
            },
            _ => None,
        }
    }
}


#[derive(Clone, Debug)]
pub enum TableKey {
    Int(i32),
    String(String),
    BigInt(i64),
    Double(f64),
}

impl TableKey {
    
    pub fn from_meta_enum(meta_type: &MetaEnum, value: &[u8]) -> Result<Self, String> {
        match meta_type {
            MetaEnum::INTEGER => {
                if value.len() < 4 {
                    return Err("Insufficient bytes for i32".to_string());
                }
                let bytes: [u8; 4] = value[0..4].try_into()
                    .map_err(|_| "Failed to convert bytes to i32")?;
                Ok(TableKey::Int(i32::from_le_bytes(bytes)))
            },
            MetaEnum::BIGINT => {
                if value.len() < 8 {
                    return Err("Insufficient bytes for i64".to_string());
                }
                let bytes: [u8; 8] = value[0..8].try_into()
                    .map_err(|_| "Failed to convert bytes to i64")?;
                Ok(TableKey::BigInt(i64::from_le_bytes(bytes)))
            },
            MetaEnum::DOUBLE => {
                if value.len() < 8 {
                    return Err("Insufficient bytes for f64".to_string());
                }
                let bytes: [u8; 8] = value[0..8].try_into()
                    .map_err(|_| "Failed to convert bytes to f64")?;
                Ok(TableKey::Double(f64::from_le_bytes(bytes)))
            },
            MetaEnum::FLOAT => {
                if value.len() < 4 {
                    return Err("Insufficient bytes for f32".to_string());
                }
                let bytes: [u8; 4] = value[0..4].try_into()
                    .map_err(|_| "Failed to convert bytes to f32")?;
                Ok(TableKey::Double(f32::from_le_bytes(bytes) as f64))
            },
            MetaEnum::STRING(_) => {
                if value.len() < 4 {
                    return Err("Insufficient bytes for string length".to_string());
                }
                let len_bytes: [u8; 4] = value[0..4].try_into()
                    .map_err(|_| "Failed to read string length")?;
                let len = i32::from_le_bytes(len_bytes) as usize;
                
                if value.len() < 4 + len {
                    return Err("Insufficient bytes for string data".to_string());
                }
                
                let string_bytes = &value[4..4 + len];
                let string_val = String::from_utf8(string_bytes.to_vec())
                    .map_err(|_| "Invalid UTF-8 in string")?;
                Ok(TableKey::String(string_val))
            },
        }
    }
}


pub struct TableBTreeManager {
    
    table_trees: HashMap<i32, TableBTree>,
    
    table_key_types: HashMap<i32, MetaEnum>,
}

impl TableBTreeManager {
    pub fn new() -> Self {
        TableBTreeManager {
            table_trees: HashMap::new(),
            table_key_types: HashMap::new(),
        }
    }

    
    pub fn register_table(&mut self, table_id: i32, primary_key_type: MetaEnum) {
        let btree = TableBTree::new(&primary_key_type);
        self.table_trees.insert(table_id, btree);
        self.table_key_types.insert(table_id, primary_key_type);
    }

    
    pub fn insert(&mut self, table_id: i32, key_value: TableKey, page_id: i64, offset: i32) -> Result<(), String> {
        let tree = self.table_trees.get_mut(&table_id)
            .ok_or_else(|| format!("Table {} not found", table_id))?;
        
        tree.insert(key_value, page_id, offset)
    }

    
    pub fn search(&self, table_id: i32, key_value: &TableKey) -> Result<Option<Box<data>>, String> {
        let tree = self.table_trees.get(&table_id)
            .ok_or_else(|| format!("Table {} not found", table_id))?;
        
        Ok(tree.search(key_value))
    }

    
    pub fn table_exists(&self, table_id: i32) -> bool {
        self.table_trees.contains_key(&table_id)
    }

    
    pub fn get_primary_key_type(&self, table_id: i32) -> Option<&MetaEnum> {
        self.table_key_types.get(&table_id)
    }

    
    pub fn get_table_ids(&self) -> Vec<i32> {
        self.table_trees.keys().copied().collect()
    }
}


pub static BTREE_MANAGER: Mutex<Option<TableBTreeManager>> = Mutex::new(None);


pub fn initialize_btree_manager() {
    let mut manager_lock = BTREE_MANAGER.lock().unwrap();
    if manager_lock.is_none() {
        *manager_lock = Some(TableBTreeManager::new());
        println!("B+Tree manager initialized");
    }
}


pub fn with_btree_manager<F, R>(f: F) -> Result<R, String>
where
    F: FnOnce(&mut TableBTreeManager) -> R,
{
    let mut manager_lock = BTREE_MANAGER.lock()
        .map_err(|_| "Failed to lock B+Tree manager")?;
    let manager = manager_lock.as_mut()
        .ok_or("B+Tree manager not initialized")?;
    Ok(f(manager))
}


pub fn register_table(table_id: i32, primary_key_type: MetaEnum) -> Result<(), String> {
    with_btree_manager(|manager| {
        manager.register_table(table_id, primary_key_type);
    })
}

pub fn insert_into_table(table_id: i32, key_value: TableKey, page_id: i64, offset: i32) -> Result<(), String> {
    with_btree_manager(|manager| {
        manager.insert(table_id, key_value, page_id, offset)
    })?
}

pub fn search_in_table(table_id: i32, key_value: &TableKey) -> Result<Option<Box<data>>, String> {
    with_btree_manager(|manager| {
        manager.search(table_id, key_value)
    })?
}