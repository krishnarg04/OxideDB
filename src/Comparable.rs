use std::cmp::Ordering;


pub trait Comparable: Clone + std::fmt::Debug {
    
    fn get_key(&self) -> Self;
    
    
    fn is_equal(&self, other: &Self) -> bool;
    
    
    fn is_greater(&self, other: &Self) -> bool;
    
    
    fn is_greater_equal(&self, other: &Self) -> bool;
    
    
    fn is_less(&self, other: &Self) -> bool;
    
    
    fn is_less_equal(&self, other: &Self) -> bool;
    
    
    fn compare(&self, other: &Self) -> Ordering {
        if self.is_equal(other) {
            Ordering::Equal
        } else if self.is_less(other) {
            Ordering::Less
        } else {
            Ordering::Greater
        }
    }
}


impl Comparable for i32 {
    fn get_key(&self) -> Self {
        *self
    }
    
    fn is_equal(&self, other: &Self) -> bool {
        self == other
    }
    
    fn is_greater(&self, other: &Self) -> bool {
        self > other
    }
    
    fn is_greater_equal(&self, other: &Self) -> bool {
        self >= other
    }
    
    fn is_less(&self, other: &Self) -> bool {
        self < other
    }
    
    fn is_less_equal(&self, other: &Self) -> bool {
        self <= other
    }
}


impl Comparable for String {
    fn get_key(&self) -> Self {
        self.clone()
    }
    
    fn is_equal(&self, other: &Self) -> bool {
        self == other
    }
    
    fn is_greater(&self, other: &Self) -> bool {
        self > other
    }
    
    fn is_greater_equal(&self, other: &Self) -> bool {
        self >= other
    }
    
    fn is_less(&self, other: &Self) -> bool {
        self < other
    }
    
    fn is_less_equal(&self, other: &Self) -> bool {
        self <= other
    }
}


impl Comparable for i64 {
    fn get_key(&self) -> Self {
        *self
    }
    
    fn is_equal(&self, other: &Self) -> bool {
        self == other
    }
    
    fn is_greater(&self, other: &Self) -> bool {
        self > other
    }
    
    fn is_greater_equal(&self, other: &Self) -> bool {
        self >= other
    }
    
    fn is_less(&self, other: &Self) -> bool {
        self < other
    }
    
    fn is_less_equal(&self, other: &Self) -> bool {
        self <= other
    }
}


impl Comparable for f64 {
    fn get_key(&self) -> Self {
        *self
    }
    
    fn is_equal(&self, other: &Self) -> bool {
        (self - other).abs() < f64::EPSILON
    }
    
    fn is_greater(&self, other: &Self) -> bool {
        self > other && !self.is_equal(other)
    }
    
    fn is_greater_equal(&self, other: &Self) -> bool {
        self > other || self.is_equal(other)
    }
    
    fn is_less(&self, other: &Self) -> bool {
        self < other && !self.is_equal(other)
    }
    
    fn is_less_equal(&self, other: &Self) -> bool {
        self < other || self.is_equal(other)
    }
}