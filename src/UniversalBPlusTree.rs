use std::sync::{Arc, RwLock};
use crate::UniversalKey::{Key, data};
use crate::Comparable::Comparable;

#[derive(Clone, Debug)]
struct Node<T: Comparable> {
    keys: Vec<Box<Key<T>>>,
    count: usize,
    size: usize,
    pointers: Vec<Option<Arc<RwLock<Box<Node<T>>>>>>, 
    next: Option<Arc<RwLock<Box<Node<T>>>>>, 
    is_leaf: bool,
}

const MAX_KEYS: usize = 3; 

pub struct BPlusTree<T: Comparable> {
    root: Option<Arc<RwLock<Box<Node<T>>>>>,
}

impl<T: Comparable + Send + Sync + 'static> BPlusTree<T> {
    pub fn new() -> BPlusTree<T> {
        BPlusTree { root: None }
    }

    pub fn insert(&mut self, node: Option<Box<Key<T>>>) {
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
            self.root = Some(Arc::new(RwLock::new(new_root)));
        }

        let root_arc = self.root.as_ref().unwrap().clone();
        if let Some((promoted_key, left_node, right_node)) = self._insert_rec(root_arc.clone(), value) {
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

            self.root = Some(Arc::new(RwLock::new(new_root)));
        }
    }

    fn _insert_rec(
        &mut self,
        current: Arc<RwLock<Box<Node<T>>>>,
        value: Box<Key<T>>,
    ) -> Option<(Box<Key<T>>, Arc<RwLock<Box<Node<T>>>>, Arc<RwLock<Box<Node<T>>>>)> {
        let is_leaf = {
            let node = current.read().unwrap();
            node.is_leaf
        };

        if is_leaf {
            self.add_new_element(&current, value);

            let count = {
                let node = current.read().unwrap();
                node.count
            };

            if count > MAX_KEYS {
                return Some(self.split_leaf(&current));
            } else {
                return None;
            }
        } else {
            let pos = Self::_binary_search(&current, &value.key);
            
            let child_opt = {
                let node = current.read().unwrap();
                node.pointers.get(pos).and_then(|p| p.as_ref().cloned())
            };

            if child_opt.is_none() {
                let new_child = Box::new(Node {
                    keys: Vec::new(),
                    count: 0,
                    size: MAX_KEYS,
                    pointers: vec![None; MAX_KEYS + 1],
                    next: None,
                    is_leaf: true,
                });
                let arc = Arc::new(RwLock::new(new_child));
                {
                    let mut node = current.write().unwrap();
                    if pos < node.pointers.len() {
                        node.pointers[pos] = Some(arc.clone());
                    }
                }
                if let Some((prom_key, left, right)) = self._insert_rec(arc, value) {
                    self.insert_into_internal(&current, prom_key, left, right);
                    let count = {
                        let node = current.read().unwrap();
                        node.count
                    };
                    if count > MAX_KEYS {
                        return Some(self.split_internal(&current));
                    }
                }
            } else {
                let child = child_opt.unwrap();
                if let Some((prom_key, left, right)) = self._insert_rec(child, value) {
                    self.insert_into_internal(&current, prom_key, left, right);
                    let count = {
                        let node = current.read().unwrap();
                        node.count
                    };
                    if count > MAX_KEYS {
                        return Some(self.split_internal(&current));
                    }
                }
            }
            return None;
        }
    }

    fn insert_into_internal(
        &mut self,
        current: &Arc<RwLock<Box<Node<T>>>>,
        promoted_key: Box<Key<T>>,
        left: Arc<RwLock<Box<Node<T>>>>,
        right: Arc<RwLock<Box<Node<T>>>>,
    ) {
        let pos = Self::_binary_search(current, &promoted_key.key);

        {
            let mut node = current.write().unwrap();
            node.keys.insert(pos, promoted_key);
            node.count += 1;

            if node.pointers.len() < node.keys.len() + 1 {
                let sz = node.keys.len();
                node.pointers.resize(sz + 1, None);
            }

            node.pointers.insert(pos + 1, Some(right));
            if pos < node.pointers.len() {
                node.pointers[pos] = Some(left);
            }

            if node.pointers.len() > MAX_KEYS + 1 {
                node.pointers.pop();
            }
        }
    }

    fn add_new_element(&mut self, current: &Arc<RwLock<Box<Node<T>>>>, value: Box<Key<T>>) {
        let pos = BPlusTree::_binary_search(current, &value.key);
        let mut node = current.write().unwrap();
        node.keys.insert(pos, value);
        node.count += 1;
    }

    fn split_leaf(
        &mut self,
        current: &Arc<RwLock<Box<Node<T>>>>,
    ) -> (Box<Key<T>>, Arc<RwLock<Box<Node<T>>>>, Arc<RwLock<Box<Node<T>>>>) {
        let mut node = current.write().unwrap();
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
        let right_arc = Arc::new(RwLock::new(right_node));

        node.next = Some(right_arc.clone());

        let promoted_key = {
            let right_node = right_arc.read().unwrap();
            right_node.keys[0].clone()
        };


        let left_arc = current.clone();

        drop(node);

        (promoted_key, left_arc, right_arc)
    }


    fn split_internal(
        &mut self,
        current: &Arc<RwLock<Box<Node<T>>>>,
    ) -> (Box<Key<T>>, Arc<RwLock<Box<Node<T>>>>, Arc<RwLock<Box<Node<T>>>>) {
        let mut node = current.write().unwrap();
        let total = node.keys.len();
        let mid_index = total / 2; 
        let promoted_key = node.keys[mid_index].clone();


        let left_keys = node.keys[..mid_index].to_vec();

        let right_keys = node.keys[mid_index + 1..].to_vec();

        let mut all_ptrs = node.pointers.clone();
        if all_ptrs.len() < total + 1 {
            all_ptrs.resize(total + 1, None);
        }

        let mut left_ptrs: Vec<Option<Arc<RwLock<Box<Node<T>>>>>> = Vec::new();
        let mut right_ptrs: Vec<Option<Arc<RwLock<Box<Node<T>>>>>> = Vec::new();

        for i in 0..=mid_index {
            if i < all_ptrs.len() {
                left_ptrs.push(all_ptrs[i].as_ref().cloned());
            }
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
                let rk_len = node.keys.len() - (mid_index + 1);
                rk_len
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

        let left_arc = Arc::new(RwLock::new(left_node));
        let right_arc = Arc::new(RwLock::new(right_node));

        drop(node);

        (promoted_key, left_arc, right_arc)
    }

    pub fn _binary_search(current: &Arc<RwLock<Box<Node<T>>>>, target: &T) -> usize {
        let node = current.read().unwrap();
        let mut low: usize = 0;
        let mut high: usize = node.count;
        while low < high {
            let mid = (low + high) / 2;
            if node.keys[mid].key.is_equal(target) {
                return mid;
            } else if node.keys[mid].key.is_less(target) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        low
    }

    pub fn search(&self, key: &T) -> Option<Box<data>> {
        if self.root.is_none() {
            return None;
        }
        let current = self.root.as_ref().unwrap().clone();
        self.search_rec(current, key)
    }

    fn search_rec(&self, current: Arc<RwLock<Box<Node<T>>>>, key: &T) -> Option<Box<data>> {
        let pos = BPlusTree::_binary_search(&current, key);
        let node = current.read().unwrap();
        if node.is_leaf {
            if pos < node.keys.len() && node.keys[pos].key.is_equal(key) {
                return node.keys[pos].data.clone();
            } else {
                return None;
            }
        } else {
            if pos < node.pointers.len() {
                if let Some(ref child) = node.pointers[pos] {
                    let child_clone = child.clone();
                    drop(node); 
                    return self.search_rec(child_clone, key);
                } else {
                    return None;
                }
            } else {
                return None;
            }
        }
    }

    pub fn print_tree(&self) {
        if let Some(ref root) = self.root {
            Self::print_rec(root, 0);
        }
    }

    fn print_rec(current: &Arc<RwLock<Box<Node<T>>>>, level: i32) {
        let node = current.read().unwrap();
        println!(" level {} Node with {} keys", level, node.keys.len());
        let pointers = node.pointers.clone();
        drop(node); 
        
        for pointer in pointers.iter() {
            if let Some( child) = pointer {
                Self::print_rec(child, level + 1);
            }
        }
    }
}

pub type IntBPlusTree = BPlusTree<i32>;
pub type StringBPlusTree = BPlusTree<String>;
pub type BigIntBPlusTree = BPlusTree<i64>;
pub type DoubleBPlusTree = BPlusTree<f64>;