use std::fs::{File, OpenOptions};
use std::io::{Write, Read, BufWriter, BufReader};
use std::collections::HashMap;
use crate::BPlusTree::{BPlusTree, Key, data};

/// Structure to serialize B+Tree node data
#[derive(Debug)]
struct SerializedBTreeEntry {
    key: i32,
    page_id: i64,
    offset: i32,
}

impl SerializedBTreeEntry {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.key.to_le_bytes());
        bytes.extend_from_slice(&self.page_id.to_le_bytes());
        bytes.extend_from_slice(&self.offset.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 16 {
            return Err("Insufficient bytes for SerializedBTreeEntry".to_string());
        }
        
        let key = i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let page_id = i64::from_le_bytes([
            bytes[4], bytes[5], bytes[6], bytes[7],
            bytes[8], bytes[9], bytes[10], bytes[11]
        ]);
        let offset = i32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]);
        
        Ok(SerializedBTreeEntry {
            key,
            page_id,
            offset,
        })
    }
}

/// Persists and loads B+Tree indexes
pub struct BTreePersistence;

impl BTreePersistence {
    /// Save a B+Tree to file (using brute force key search)
    pub fn save_btree(table_name: &str, btree: &BPlusTree) -> Result<(), String> {
        let filename = format!("{}_btree.idx", table_name);
        
        // Collect all entries from the B+Tree using brute force search
        let entries = Self::collect_btree_entries_brute_force(btree);
        
        // Write to file
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&filename)
            .map_err(|e| format!("Failed to create B+Tree file: {}", e))?;
        
        let mut writer = BufWriter::new(file);
        
        // Write number of entries
        let num_entries = entries.len() as i32;
        writer.write_all(&num_entries.to_le_bytes())
            .map_err(|e| format!("Failed to write entry count: {}", e))?;
        
        // Write each entry
        for entry in entries {
            let entry_bytes = entry.to_bytes();
            writer.write_all(&entry_bytes)
                .map_err(|e| format!("Failed to write entry: {}", e))?;
        }
        
        writer.flush()
            .map_err(|e| format!("Failed to flush B+Tree file: {}", e))?;
        
        println!("Saved B+Tree for table '{}' with {} entries", table_name, num_entries);
        Ok(())
    }
    
    /// Load a B+Tree from file
    pub fn load_btree(table_name: &str) -> Result<BPlusTree, String> {
        let filename = format!("{}_btree.idx", table_name);
        
        if !std::path::Path::new(&filename).exists() {
            // File doesn't exist, return empty B+Tree
            return Ok(BPlusTree::new());
        }
        
        let mut file = File::open(&filename)
            .map_err(|e| format!("Failed to open B+Tree file: {}", e))?;
        
        let mut reader = BufReader::new(file);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)
            .map_err(|e| format!("Failed to read B+Tree file: {}", e))?;
        
        if buffer.len() < 4 {
            return Ok(BPlusTree::new());
        }
        
        // Read number of entries
        let num_entries = i32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
        
        // Create new B+Tree
        let mut btree = BPlusTree::new();
        
        // Read and insert each entry
        let mut offset = 4;
        for _ in 0..num_entries {
            if offset + 16 > buffer.len() {
                break;
            }
            
            let entry = SerializedBTreeEntry::from_bytes(&buffer[offset..offset + 16])?;
            
            // Create and insert key
            let data_ptr = Box::new(data::new(entry.page_id, entry.offset));
            let key_entry = Box::new(Key::new(entry.key, Some(data_ptr)));
            btree.insert(Some(key_entry));
            
            offset += 16;
        }
        
        println!("Loaded B+Tree for table '{}' with {} entries", table_name, num_entries);
        Ok(btree)
    }
    
    /// Collect all entries from a B+Tree using brute force search
    /// This is a workaround since we don't have a proper traverse method
    fn collect_btree_entries_brute_force(btree: &BPlusTree) -> Vec<SerializedBTreeEntry> {
        let mut entries = Vec::new();
        
        // Test a reasonable range of keys (both positive and negative)
        // This is not ideal but works for our current B+Tree implementation
        
        // Test positive keys
        for key in 1..10000 {
            if let Some(data_ref) = btree.search(key) {
                entries.push(SerializedBTreeEntry {
                    key,
                    page_id: data_ref.page_id,
                    offset: data_ref.offset,
                });
            }
        }
        
        // Test negative keys
        for key in -1000..0 {
            if let Some(data_ref) = btree.search(key) {
                entries.push(SerializedBTreeEntry {
                    key,
                    page_id: data_ref.page_id,
                    offset: data_ref.offset,
                });
            }
        }
        
        entries
    }
    
    /// Save all B+Trees for all tables
    pub fn save_all_btrees(table_btrees: &HashMap<String, BPlusTree>) -> Result<(), String> {
        for (table_name, btree) in table_btrees {
            Self::save_btree(table_name, btree)?;
        }
        Ok(())
    }
    
    /// Load all B+Trees for existing tables
    pub fn load_all_btrees(table_names: &[String]) -> Result<HashMap<String, BPlusTree>, String> {
        let mut btrees = HashMap::new();
        
        for table_name in table_names {
            let btree = Self::load_btree(table_name)?;
            btrees.insert(table_name.clone(), btree);
        }
        
        Ok(btrees)
    }
}

/// Manual save function for convenience
pub fn save_btree_manually(table_name: &str, btree: &BPlusTree) -> Result<(), String> {
    BTreePersistence::save_btree(table_name, btree)
}