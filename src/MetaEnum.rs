 #[derive(Clone)]
pub enum MetaEnum {
    INTEGER,
    FLOAT,
    DOUBLE,
    BIGINT,
    STRING(i64),
}

impl MetaEnum {
    pub fn size(&self) -> usize {
        match self {
            MetaEnum::INTEGER => 4,
            MetaEnum::FLOAT => 4,
            MetaEnum::DOUBLE => 8,
            MetaEnum::BIGINT => 8,
            MetaEnum::STRING(len) => *len as usize,
        }
    }

    pub fn get_total_size(metadata: &Vec<MetaEnum>) -> usize {
        metadata.iter().map(|meta| meta.size()).sum()
    }
}


pub enum DataArray {
    INTEGER(i32),
    FLOAT(f32),
    DOUBLE(f64),
    BIGINT(i64),
    STRING(String, i32), 
}

pub struct row_array {
    pub meta_data: Vec<MetaEnum>,
    pub data: Vec<DataArray>,
}

impl row_array {
    pub fn new() -> Self {
        row_array { meta_data: Vec::new(), data: Vec::new() }
    }
    pub fn add_meta(&mut self, meta: MetaEnum) {
        self.meta_data.push(meta);
    }
    pub fn add_meta_array(&mut self, meta: &Vec<MetaEnum>) {
        self.meta_data.extend(meta.clone());
    }

    pub fn add_data(&mut self, data: DataArray) {
        self.data.push(data);
    }

    pub fn add_array(&mut self, data: Vec<DataArray>) {
        self.data.extend(data);
    }

    pub fn get_data(&self) -> &Vec<DataArray> {
        &self.data
    }

    pub fn get_data_as_string(&self) -> String {
        self.data.iter().map(|d| match d {
            DataArray::INTEGER(i) => i.to_string(),
            DataArray::FLOAT(f) => f.to_string(),
            DataArray::DOUBLE(d) => d.to_string(),
            DataArray::BIGINT(b) => b.to_string(),
            DataArray::STRING(s, _) => s.clone(),
        }).collect::<Vec<String>>().join(", ")
    }
    pub fn get_data_as_bytes(&self) -> Vec<u8> {
    let size = MetaEnum::get_total_size(&self.meta_data);
    let mut bytes = Vec::with_capacity(size);

    for data in &self.data {
        match data {
            DataArray::INTEGER(i) => bytes.extend_from_slice(&i.to_le_bytes()),
            DataArray::FLOAT(f) => bytes.extend_from_slice(&f.to_le_bytes()),
            DataArray::DOUBLE(d) => bytes.extend_from_slice(&d.to_le_bytes()),
            DataArray::BIGINT(b) => bytes.extend_from_slice(&b.to_le_bytes()),
            DataArray::STRING(s, _) => {
                let len = s.len() as i32;
                bytes.extend_from_slice(&len.to_le_bytes());
                bytes.extend_from_slice(s.as_bytes());
            }
        }
    }
    bytes
}
}


