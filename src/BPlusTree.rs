use std::cell::RefCell;
use std::rc::Rc;

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
pub struct Key {
    pub key: i32,
    pub data: Option<Box<data>>,
}

impl Key {
    pub fn new(key: i32, data: Option<Box<data>>) -> Key {
        Key { key, data }
    }
    fn get_key(&self) -> i32 {
        self.key
    }
}

#[derive(Clone, Debug)]
struct Node {
    keys: Vec<Box<Key>>,
    count: usize,
    size: usize,
    pointers: Vec<Option<Rc<RefCell<Box<Node>>>>>, 
    next: Option<Rc<RefCell<Box<Node>>>>, 
    is_leaf: bool,
}

const MAX_KEYS: usize = 3; 

pub struct BPlusTree {
    root: Option<Rc<RefCell<Box<Node>>>>,
}

impl BPlusTree {
    pub fn new() -> BPlusTree {
        BPlusTree { root: None }
    }

    
    pub fn insert(&mut self, node: Option<Box<Key>>) {
        if node.is_none() {
            return;
        }
        let value = node.unwrap();
        if self.root.is_none() {
            
            let new_root = Box::new(Node {
                keys: Vec::new(),
                count: 0,
                size: MAX_KEYS,
                pointers: vec![None; MAX_KEYS + 1],
                next: None,
                is_leaf: true,
            });
            self.root = Some(Rc::new(RefCell::new(new_root)));
        }

        
        let root_rc = self.root.as_ref().unwrap().clone();
        if let Some((promoted_key, left_node, right_node)) = self._insert_rec(root_rc.clone(), value) {
            
            let mut new_root = Box::new(Node {
                keys: Vec::new(),
                count: 0,
                size: MAX_KEYS,
                pointers: vec![None; MAX_KEYS + 1],
                next: None,
                is_leaf: false,
            });

            
            new_root.keys.push(promoted_key);
            new_root.count = 1;
            
            new_root.pointers[0] = Some(left_node);
            new_root.pointers[1] = Some(right_node);

            self.root = Some(Rc::new(RefCell::new(new_root)));
        }

    }

    
    fn _insert_rec(
        &mut self,
        current: Rc<RefCell<Box<Node>>>,
        value: Box<Key>,
    ) -> Option<(Box<Key>, Rc<RefCell<Box<Node>>>, Rc<RefCell<Box<Node>>>)>

    {
        
        if current.borrow().is_leaf {
            self.add_new_element(&current, value);

            if current.borrow().count > MAX_KEYS {
                
                return Some(self.split_leaf(&current));
            } else {
                return None;
            }
        } else {
            
            let pos = Self::_binary_search(&current, value.get_key());
            
            
            
            
            let child_opt = current.borrow().pointers[pos].as_ref().cloned();
            if child_opt.is_none() {
                
                let new_child = Box::new(Node {
                    keys: Vec::new(),
                    count: 0,
                    size: MAX_KEYS,
                    pointers: vec![None; MAX_KEYS + 1],
                    next: None,
                    is_leaf: true,
                });
                let rc = Rc::new(RefCell::new(new_child));
                current.borrow_mut().pointers[pos] = Some(rc.clone());
                
                if let Some((prom_key, left, right)) = self._insert_rec(rc, value) {
                    
                    self.insert_into_internal(&current, prom_key, left, right);
                    if current.borrow().count > MAX_KEYS {
                        return Some(self.split_internal(&current));
                    }
                }
            } else {
                let child = child_opt.unwrap();
                if let Some((prom_key, left, right)) = self._insert_rec(child, value) {
                    
                    self.insert_into_internal(&current, prom_key, left, right);
                    if current.borrow().count > MAX_KEYS {
                        return Some(self.split_internal(&current));
                    }
                }
            }
            return None;
        }
    }

    
    fn insert_into_internal(
        &mut self,
        current: &Rc<RefCell<Box<Node>>>,
        promoted_key: Box<Key>,
        left: Rc<RefCell<Box<Node>>>,
        right: Rc<RefCell<Box<Node>>>,
    ) {
        let pos = Self::_binary_search(current, promoted_key.get_key());

        {
            let mut node = current.borrow_mut();
            node.keys.insert(pos, promoted_key);
            node.count += 1;

            
            if node.pointers.len() < node.keys.len() + 1 {
                let sz = node.keys.len();
                node.pointers.resize(sz + 1, None);
            }

            
            node.pointers.insert(pos + 1, Some(right));
            node.pointers[pos] = Some(left);

            
            if node.pointers.len() > MAX_KEYS + 1 {
                node.pointers.pop();
            }
        }
    }


    
    fn add_new_element(&mut self, current: &Rc<RefCell<Box<Node>>>, value: Box<Key>) {
        let pos = BPlusTree::_binary_search(current, value.get_key());
        current.borrow_mut().keys.insert(pos, value);
        current.borrow_mut().count += 1;
    }

    
    
    fn split_leaf(
        &mut self,
        current: &Rc<RefCell<Box<Node>>>,
    ) -> (Box<Key>, Rc<RefCell<Box<Node>>>, Rc<RefCell<Box<Node>>>) {
        let mut node = current.borrow_mut();
        let total = node.keys.len();
        let mid = (total + 1) / 2; 

        let right_keys = node.keys.split_off(mid);
        let left_keys = node.keys.clone(); 
        let left_count = left_keys.len();
        let right_count = right_keys.len();

        
        node.keys = left_keys;
        node.count = left_count;
        
        
        let right_node = Box::new(Node {
            keys: right_keys,
            count: right_count,
            size: MAX_KEYS,
            pointers: vec![None; MAX_KEYS + 1],
            next: node.next.clone(),
            is_leaf: true,
        });
        let right_rc = Rc::new(RefCell::new(right_node));

        
        node.next = Some(right_rc.clone());

        
        let promoted_key = right_rc.borrow().keys[0].clone();

        
        
        let left_rc = current.clone();

        (promoted_key, left_rc, right_rc)
    }

    
    fn split_internal(
        &mut self,
        current: &Rc<RefCell<Box<Node>>>,
    ) -> (Box<Key>, Rc<RefCell<Box<Node>>>, Rc<RefCell<Box<Node>>>) {
        let mut node = current.borrow_mut();
        let total = node.keys.len();
        
        let mid_index = total / 2; 
        let promoted_key = node.keys[mid_index].clone();

        
        let left_keys = node.keys[..mid_index].to_vec();
        
        let right_keys = node.keys[mid_index + 1..].to_vec();

        
        let mut left_ptrs: Vec<Option<Rc<RefCell<Box<Node>>>>> = Vec::new();
        let mut right_ptrs: Vec<Option<Rc<RefCell<Box<Node>>>>> = Vec::new();

        
        let mut all_ptrs = node.pointers.clone();
        if all_ptrs.len() < total + 1 {
            all_ptrs.resize(total + 1, None);
        }

        
        for i in 0..=mid_index {
            left_ptrs.push(all_ptrs[i].as_ref().cloned());
        }
        
        for i in (mid_index + 1)..all_ptrs.len() {
            right_ptrs.push(all_ptrs[i].as_ref().cloned());
        }

        
        let left_node = Box::new(Node {
            keys: left_keys,
            count: left_ptrs.iter().filter(|p| p.is_some()).count(), 
            size: MAX_KEYS,
            pointers: {
                let mut v = left_ptrs;
                v.resize(MAX_KEYS + 1, None);
                v
            },
            next: None,
            is_leaf: false,
        });
        let right_node = Box::new(Node {
            keys: right_keys,
            count: {
                
                let cnt = {
                    
                    let rk_len = node.keys.len() - (mid_index + 1);
                    rk_len
                };
                cnt
            },
            size: MAX_KEYS,
            pointers: {
                let mut v = right_ptrs;
                v.resize(MAX_KEYS + 1, None);
                v
            },
            next: None,
            is_leaf: false,
        });

        let left_rc = Rc::new(RefCell::new(left_node));
        let right_rc = Rc::new(RefCell::new(right_node));

        
        
        
        
        

        (promoted_key, left_rc, right_rc)
    }

    
    pub fn _binary_search(current: &Rc<RefCell<Box<Node>>>, target: i32) -> usize {
        let node = current.borrow();
        let mut low: usize = 0;
        let mut high: usize = node.count;
        while low < high {
            let mid = (low + high) / 2;
            if node.keys[mid].get_key() == target {
                return mid;
            } else if node.keys[mid].get_key() < target {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        low
    }

    
    pub fn search(&self, key: i32) -> Option<Box<data>> {
        if self.root.is_none() {
            return None;
        }
        let current = self.root.as_ref().unwrap().clone();
        self.search_rec(current, key)
    }

    fn search_rec(&self, current: Rc<RefCell<Box<Node>>>, key: i32) -> Option<Box<data>> {
        let pos = BPlusTree::_binary_search(&current, key);
        let node = current.borrow();
        if node.is_leaf {
            if pos < node.keys.len() && node.keys[pos].get_key() == key {
                return node.keys[pos].data.clone();
            } else {
                return None;
            }
        } else {
            
            
            if pos < node.pointers.len() {
                if let Some(ref child) = node.pointers[pos] {
                    return self.search_rec(child.clone(), key);
                } else {
                    return None;
                }
            } else {
                return None;
            }
        }
    }

    fn print_tree(&self) {
        fn print_rec(current: &Rc<RefCell<Box<Node>>>, value: i32) {
            let node = current.borrow();
            println!(" level {} Node: {:?}", value, node.keys.iter().map(|k| k.get_key()).collect::<Vec<i32>>());
            for i in 0..4 {
                if !node.pointers[i].is_none() {
                    if let Some(ref pointer) = node.pointers[i] {
                        print_rec(pointer, value + 1);
                    }
                }
            }
        }
        print_rec(self.root.as_ref().unwrap(), 0);
    }
}
