use std::collections::HashMap;
use std::rc::{Rc, Weak};
use std::cell::RefCell;
use crate::RowData::RawData;

pub struct LRUDict {
    dict: HashMap<i64, Rc<RefCell<DoublyLinkedListNode>>>,
    list: DoublyLinkedList,
    capacity: usize,
}

struct DoublyLinkedList {
    head: Option<Rc<RefCell<DoublyLinkedListNode>>>,
    tail: Option<Rc<RefCell<DoublyLinkedListNode>>>,
}

struct DoublyLinkedListNode {
    key: i64,
    value: Box<RawData>,
    prev: Option<Weak<RefCell<DoublyLinkedListNode>>>,
    next: Option<Rc<RefCell<DoublyLinkedListNode>>>,
}

impl DoublyLinkedList {
    fn new() -> Self {
        DoublyLinkedList { head: None, tail: None }
    }

    fn push_to_tail(&mut self, key: i64, value: Box<RawData>) -> Rc<RefCell<DoublyLinkedListNode>> {
        let new_node = Rc::new(RefCell::new(DoublyLinkedListNode {
            key,
            value,
            prev: self.tail.as_ref().map(Rc::downgrade), 
            next: None,
        }));

        match self.tail.take() {
            Some(old_tail) => {
                old_tail.borrow_mut().next = Some(new_node.clone());
            }
            None => {
                self.head = Some(new_node.clone());
            }
        }

        self.tail = Some(new_node.clone());
        new_node
    }

    fn unlink_node(&mut self, node: &Rc<RefCell<DoublyLinkedListNode>>) {
        let node_ref = node.borrow();
        let prev_node = node_ref.prev.as_ref().and_then(Weak::upgrade);
        let next_node = node_ref.next.clone();

        match (prev_node, next_node) {
            (Some(prev), Some(next)) => { 
                prev.borrow_mut().next = Some(next.clone());
                next.borrow_mut().prev = Some(Rc::downgrade(&prev));
            }
            (Some(prev), None) => { 
                prev.borrow_mut().next = None;
                self.tail = Some(prev);
            }
            (None, Some(next)) => { 
                next.borrow_mut().prev = None;
                self.head = Some(next);
            }
            (None, None) => { 
                self.head = None;
                self.tail = None;
            }
        }
    }

    fn move_to_tail(&mut self, node: &Rc<RefCell<DoublyLinkedListNode>>) {
        if let Some(tail) = &self.tail {
            if Rc::ptr_eq(node, tail) {
                return;
            }
        }

        self.unlink_node(node);

        let old_tail = self.tail.take().unwrap(); // We know the list isn't empty.
        node.borrow_mut().prev = Some(Rc::downgrade(&old_tail));
        node.borrow_mut().next = None;
        old_tail.borrow_mut().next = Some(node.clone());
        self.tail = Some(node.clone());
    }
}

impl LRUDict {
    pub fn new(capacity: usize) -> Self {
        LRUDict {
            dict: HashMap::new(),
            list: DoublyLinkedList::new(),
            capacity,
        }
    }

    pub fn add_element(&mut self, key: i64, value: Box<RawData>) {
        if let Some(existing_node) = self.dict.get(&key) {
            existing_node.borrow_mut().value = value;
            self.list.move_to_tail(existing_node);
        } else {
            if self.dict.len() >= self.capacity {
                self.remove_lru();
            }

            let new_node = self.list.push_to_tail(key, value);
            self.dict.insert(key, new_node);
        }
    }

    pub fn get(&mut self, key: i64) -> Option<Box<RawData>> {
        if let Some(node) = self.dict.get(&key) {
            self.list.move_to_tail(node);
            Some(node.borrow().value.clone())
        } else {
            
            None
        }
    }

    fn remove_lru(&mut self) {
        if let Some(lru_node) = self.list.head.clone() {
            self.dict.remove(&lru_node.borrow().key);
            self.list.unlink_node(&lru_node);
        }
    }
}