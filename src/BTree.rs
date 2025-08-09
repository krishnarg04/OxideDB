
#[derive(Clone)]
pub struct data {
    pub page_id: i64,
    pub offset: i32,
}

impl data {
    pub fn new(page_id: i64, offset: i32) -> data {
        data { page_id, offset }
    }
}

#[derive(Clone)]
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

#[derive(Clone)]
struct Node {
    keys: Vec<Box<Key>>,
    count: usize,
    size: usize,
    pointers: Vec<Option<Box<Node>>>,
}

const MAX_KEYS: usize = 3;

pub struct BTree {
    root: Option<Box<Node>>,
    split_node_list: Vec<Box<Node>>,
    node_stack: Vec<Option<Box<Key>>>,
}

impl BTree {
    pub fn new() -> BTree {
        BTree {
            root: None,
            split_node_list: Vec::new(),
            node_stack: Vec::new(),
        }
    }

    pub fn insert(&mut self, mut node: Option<Box<Key>>) {
        if self.root.is_none() {
            let new_root = Box::new(Node {
                keys: Vec::new(),
                count: 0,
                size: MAX_KEYS,
                pointers: vec![None; MAX_KEYS + 1],
            });
            self.root = Some(new_root);
        }
        let mut troot = self.root.take().unwrap();
        self._insert_rec(&mut troot, node.unwrap());
        if self.split_node_list.len() > 0 {
           if self.split_node_list.len() > 0 {
            
                let mut new_root = Box::new(Node {
                    keys: Vec::new(),
                    count: 0,
                    size: MAX_KEYS,
                    pointers: vec![None; MAX_KEYS + 1],
                });

                let value : Box<Key> = self.node_stack.pop().unwrap().unwrap();
                self.add_element(&mut new_root, value);
                let left = self.split_node_list.pop().unwrap();
                let right = self.split_node_list.pop().unwrap();
                self.copy_point(&mut new_root, left, right);
                self.root = Some(new_root);
                return;
            }
        }
        self.root = Some(troot);

    }

    fn _insert_rec( &mut self, mut current: &mut Box<Node>, mut value : Box<Key>) {
        let mid_pos = BTree::_binary_search(current, value.get_key());
        let mut flag: bool = false;
        if current.keys.len() > mid_pos && current.keys[mid_pos].get_key() > value.get_key(){
            if current.pointers[mid_pos+1].is_none() {
                self.add_element(current, value);
            }
            else{
                self._insert_rec(current.pointers[mid_pos+1].as_mut().unwrap(), value);
                flag = true;
            }
        }
        else if current.keys.len() > mid_pos && current.keys[mid_pos].get_key() < value.get_key() {
            if current.pointers[mid_pos].is_none() {
                self.add_element(current, value);
            } else {
                self._insert_rec(current.pointers[mid_pos].as_mut().unwrap(), value);
                flag = true;
            }
        }
        else if current.keys.len() == mid_pos {
            if current.pointers[mid_pos].is_none() {
                self.add_element(current, value);
            } else {
                self._insert_rec(current.pointers[mid_pos].as_mut().unwrap(), value);
                flag = true;
            }
        } else {
            self.add_element(current, value);
        }
        
        if flag {
            if self.split_node_list.len() > 0 {
                let mut value = self.node_stack.pop().unwrap().unwrap();
                self.add_element(current, value);
                let left = self.split_node_list.pop().unwrap();
                let right = self.split_node_list.pop().unwrap();
                self.copy_point(current, left, right);
            }
        }
    }

    fn add_element(&mut self, mut current: &mut Box<Node>, mut value: Box<Key>) {
        if current.count < MAX_KEYS {
           self.add_new_element(current, value);
        } else {
            self.split_node(current, value);
        }
    }

    fn split_node(&mut self, mut current: &mut Box<Node>, mut value: Box<Key>) {
        let mid_pos = current.count / 2;
        let mut new_Vec : Vec<Box<Key>> = current.keys.to_vec();
        new_Vec.push(value);
        new_Vec.sort_by(|a, b| a.get_key().cmp(&b.get_key()));

        // left node
        let mut left_node = Box::new(Node {
            keys: new_Vec[..mid_pos].to_vec(),
            count: new_Vec[..mid_pos].len(),
            size: MAX_KEYS,
            pointers: vec![None; MAX_KEYS + 1],
        });

        // right node
        let mut right_node = Box::new(Node {
            keys: new_Vec[mid_pos+1..].to_vec(),
            count: new_Vec[mid_pos+1..].len(),
            size: MAX_KEYS,
            pointers: vec![None; MAX_KEYS + 1],
        });
        let mut new_value = new_Vec.get(mid_pos).unwrap().clone();
        self.split_node_list.push(left_node);
        self.split_node_list.push(right_node);
        self.node_stack.push(Some(new_value));
      
    }

    fn copy_values(&mut self, mut current: &mut Box<Node>, new_values : Vec<Box<Key>>) {
        current.keys.clear();
        current.count = 0;
        for key in new_values {
            current.keys.push(key);
            current.count += 1;
        }
    }

    fn add_new_element(&mut self, mut current: &mut Box<Node>, mut value: Box<Key>){
        let pos = BTree::_binary_search(current, value.get_key());
        current.keys.insert(pos, value);
        current.count += 1;
    }

    pub fn _binary_search(mut current: & Box<Node>, target: i32) -> usize{
        let mut low =0;
        let mut high = current.count;
        while low < high {
            let mid = (low + high) / 2;
            if current.keys[mid].get_key() == target {
                return mid;
            } else if current.keys[mid].get_key() < target {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        low
    }

    fn copy_point(&mut self, mut current: &mut Box<Node>,  right: Box<Node>, left: Box<Node>) {
        for i in 0..current.count {
            if current.keys[i].get_key() < right.keys[0].get_key() && current.keys[i].get_key() > left.keys[0].get_key() {
                current.pointers[i + 1] = Some(right);
                current.pointers[i] = Some(left);
                return;
            }
        }
    }

    pub fn search_elemnt<'a>(&'a self, key : i32) -> Option<&'a Box<data>> {
        if self.root.is_none() {
            println!("BTree is empty");
            return None;
        }
        let current = self.root.as_ref().unwrap();
        self.search_rec(current, key)
    }

    fn search_rec<'a>(&'a self, current: &'a Box<Node>, key:i32) -> Option<&'a Box<data>> {
        let pos = BTree::_binary_search(current, key);
        if pos < current.count && current.keys[pos].get_key() == key {
            return current.keys[pos].data.as_ref();
        } else if pos < current.count && current.keys[pos].get_key() > key {
            if current.pointers[pos + 1].is_none() {
                return None;
            } else {
                return self.search_rec(current.pointers[pos + 1].as_ref().unwrap(), key);
            }
        } else if pos < current.count && current.keys[pos].get_key() < key {
            if current.pointers[pos].is_none() {
                return None;
            } else {
                return self.search_rec(current.pointers[pos].as_ref().unwrap(), key);
            }
        } else {
            None
        }
    }
}
