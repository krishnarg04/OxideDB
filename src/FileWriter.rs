
use crate::{MetaEnum, RowData::RawData, TableMetaHandler};
use std::io::{Seek, Write, Read};
pub struct File_Handler{
    schema_name: String,
}

impl File_Handler {
    pub fn new(schema_name: String, ) -> File_Handler {
        File_Handler { schema_name }
    }

    pub fn write_to_file(&self, raw_data: &RawData) {
        let file_name = format!("{}.dat", self.schema_name);
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true) 
            .open(&file_name)
            .expect("Unable to open or create file");

        let required_file_size = (raw_data.page_id + 1) * raw_data.page_size as u64;
        let current_file_size = file.metadata().expect("Unable to get file metadata").len();

        if required_file_size > current_file_size {
            file.set_len(required_file_size)
                .expect("Unable to extend file size");
        }
        let start_pos = raw_data.page_id * raw_data.page_size as u64;
        file.seek(std::io::SeekFrom::Start(start_pos))
            .expect("Unable to seek in file");
        file.write_all(&raw_data.data)
            .expect("Unable to write data to file");

        println!("Data for page {} written to file: {}", raw_data.page_id, file_name);
    }

    pub fn read_from_file(schema_name: String, page_id: u64, page_size: usize) -> RawData {
        let file_name = format!("{}.dat", schema_name);
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .open(&file_name)
            .expect("Unable to open file");

        let start_pos = page_id * page_size as u64;
        file.seek(std::io::SeekFrom::Start(start_pos))
            .expect("Unable to seek in file");

        let mut data = vec![0; page_size];
        file.read_exact(&mut data)
            .expect("Unable to read data from file");
        
        let mut guard = TableMetaHandler::meta_config.lock().unwrap();

        let config = guard.as_mut().unwrap();
        let meta = config.get_table_meta_by_name(&schema_name)
            .expect("Table metadata not found");

        RawData::new(schema_name.clone(), meta.clone(), page_size, 0, page_id, data.into_boxed_slice())
    }
}