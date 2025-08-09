use std::mem;

use crate::MetaEnum::MetaEnum;



#[derive(Clone)]
pub struct RawData {
    pub schema_name: String,
    pub meta_data : Vec<MetaEnum>,
    pub page_size: usize,
    pub header_size: usize,
    pub page_id : u64,
    pub data : Box<[u8]>,
}

impl RawData {
    pub fn new(schema_name: String, meta_data: Vec<MetaEnum>, page_size: usize, header_size: usize, page_id: u64, data: Box<[u8]>) -> RawData {
        RawData {
            schema_name,
            meta_data,
            page_size,
            header_size,
            page_id,
            data,
        }
    }

     pub fn new_without_array(schema_name: String, meta_data: &Vec<MetaEnum>, page_size: usize, header_size: usize, page_id: u64) -> RawData {
        RawData {
            schema_name,
            meta_data: meta_data.clone(),
            page_size,
            header_size,
            page_id,
             data: vec![0; page_size].into_boxed_slice(),
        }
    }

    pub fn add_new_row(&mut self, row_data: &[u8]) {

    const OFFSET_SIZE: usize = mem::size_of::<i32>();
    
    let row_count_bytes: [u8; OFFSET_SIZE] = self.data[self.header_size..self.header_size + OFFSET_SIZE]
        .try_into()
        .expect("Failed to read row count");
    let row_count = i32::from_le_bytes(row_count_bytes);
    let slot_array_start = self.header_size + OFFSET_SIZE;
    
    let mut last_row_offset = self.page_size as i32;
    if row_count > 0 {
        let last_slot_start = slot_array_start + (row_count as usize - 1) * OFFSET_SIZE;
        let last_slot_end = last_slot_start + OFFSET_SIZE;
        
        let offset_bytes: [u8; OFFSET_SIZE] = self.data[last_slot_start..last_slot_end]
            .try_into()
            .expect("Failed to read last row offset");
        last_row_offset = i32::from_le_bytes(offset_bytes);
    }
    
    let new_row_offset = last_row_offset - (row_data.len() as i32);

    let new_row_count = row_count + 1;
    self.data[self.header_size..self.header_size + OFFSET_SIZE]
        .copy_from_slice(&new_row_count.to_le_bytes());

    let new_slot_start = slot_array_start + (row_count as usize) * OFFSET_SIZE;
    let new_slot_end = new_slot_start + OFFSET_SIZE;
    self.data[new_slot_start..new_slot_end]
        .copy_from_slice(&new_row_offset.to_le_bytes());
    let new_row_start = new_row_offset as usize;
    let new_row_end = new_row_start + row_data.len();
    self.data[new_row_start..new_row_end].copy_from_slice(row_data);
}

pub fn data_as_str(&self, offset: usize) -> String {
    const OFFSET_SIZE: usize = mem::size_of::<i32>();
    let row_count_bytes: [u8; OFFSET_SIZE] = self.data[self.header_size..self.header_size + OFFSET_SIZE]
        .try_into()
        .expect("Failed to read row count");
    let row_count = i32::from_le_bytes(row_count_bytes);
    
    if offset >= row_count as usize {
        return String::new();
    }

    let slot_array_start = self.header_size + OFFSET_SIZE;

    let row_start_in_slot = slot_array_start + (offset * OFFSET_SIZE);
    let row_end_in_slot = row_start_in_slot + OFFSET_SIZE;
    let row_data_start = i32::from_le_bytes(self.data[row_start_in_slot..row_end_in_slot].try_into().unwrap());

    let row_data_end = if offset == 0 {
        self.page_size as i32
    } else {
        let prev_row_start_in_slot = slot_array_start + ((offset - 1) * OFFSET_SIZE);
        let prev_row_end_in_slot = prev_row_start_in_slot + OFFSET_SIZE;
        i32::from_le_bytes(self.data[prev_row_start_in_slot..prev_row_end_in_slot].try_into().unwrap())
    };

    let row_data_slice = &self.data[row_data_start as usize..row_data_end as usize];
    

    let mut result = String::new();
    let mut current_pos = 0; 

    for meta in self.meta_data.iter() {
        match meta {
            MetaEnum::INTEGER => {
                let bytes: [u8; 4] = row_data_slice[current_pos..current_pos + 4].try_into().unwrap();
                result.push_str(&format!("INTEGER: {}, ", i32::from_le_bytes(bytes)));
                current_pos += 4;
            }
            MetaEnum::FLOAT => {
                let bytes: [u8; 4] = row_data_slice[current_pos..current_pos + 4].try_into().unwrap();
                result.push_str(&format!("FLOAT: {}, ", f32::from_le_bytes(bytes)));
                current_pos += 4;
            }
            MetaEnum::DOUBLE => {
                let bytes: [u8; 8] = row_data_slice[current_pos..current_pos + 8].try_into().unwrap();
                result.push_str(&format!("DOUBLE: {}, ", f64::from_le_bytes(bytes)));
                current_pos += 8;
            }
            MetaEnum::BIGINT => {
                let bytes: [u8; 8] = row_data_slice[current_pos..current_pos + 8].try_into().unwrap();
                result.push_str(&format!("BIGINT: {}, ", i64::from_le_bytes(bytes)));
                current_pos += 8;
            }
            MetaEnum::STRING(s) => {
                let len_bytes: [u8; 4] = row_data_slice[current_pos..current_pos + 4].try_into().unwrap();
                let len = i32::from_le_bytes(len_bytes) as usize;
                current_pos += 4; 

                let str_value = String::from_utf8_lossy(&row_data_slice[current_pos..current_pos + len]);
                result.push_str(&format!("STRING: {}, ", str_value));
                current_pos += len;
            }
        }
    }
    
    result
}

    fn get_row_size(&self) -> usize {
        self.meta_data.iter().map(|meta| meta.size()).sum()
    }
}