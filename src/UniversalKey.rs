use crate::Comparable::Comparable;

#[derive(Clone, Debug)]
pub struct data {
    pub page_id: i64,
    pub offset: i32,
}

impl data {
    pub fn new(page_id: i64, offset: i32) -> data {
        data { page_id, offset }
    }
}

#[derive(Clone, Debug)]
pub struct Key<T: Comparable> {
    pub key: T,
    pub data: Option<Box<data>>,
}

impl<T: Comparable> Key<T> {
    pub fn new(key: T, data: Option<Box<data>>) -> Key<T> {
        Key { key, data }
    }
    
    pub fn get_key(&self) -> &T {
        &self.key
    }
    
    pub fn compare(&self, other: &Key<T>) -> std::cmp::Ordering {
        self.key.compare(&other.key)
    }
    
    pub fn equals(&self, other: &Key<T>) -> bool {
        self.key.is_equal(&other.key)
    }
    
    pub fn is_greater(&self, other: &Key<T>) -> bool {
        self.key.is_greater(&other.key)
    }
    
    pub fn is_less(&self, other: &Key<T>) -> bool {
        self.key.is_less(&other.key)
    }
    
    pub fn is_greater_equal(&self, other: &Key<T>) -> bool {
        self.key.is_greater_equal(&other.key)
    }
    
    pub fn is_less_equal(&self, other: &Key<T>) -> bool {
        self.key.is_less_equal(&other.key)
    }
}

pub type IntKey = Key<i32>;
pub type StringKey = Key<String>;
pub type BigIntKey = Key<i64>;
pub type DoubleKey = Key<f64>;