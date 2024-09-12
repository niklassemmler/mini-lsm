#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::key::Key;
use std::cmp::{self};
use std::collections::BinaryHeap;

use anyhow::Result;
use serde_json::from_slice;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut heap: BinaryHeap<HeapWrapper<I>> = BinaryHeap::new();
        for iter in iters.into_iter().enumerate() {
            heap.push(HeapWrapper(iter.0, iter.1));
        }
        Self { iters: heap }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.iters.peek().as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.iters.peek().as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.iters.peek().is_some()
    }

    fn next(&mut self) -> Result<()> {
        // if the current iterator is valid, push it back to the heap
        let key = self.iters.peek().as_ref().unwrap().1.key();
        println!("START");
        for x in self.iters.iter() {
            println!("{:?}/{:?}", std::str::from_utf8(x.1.key().raw_ref()), std::str::from_utf8(x.1.value()));
        }
        println!("-----");
        let mut key2: Vec<u8> = vec![0; key.len()];
        key2.copy_from_slice(key.raw_ref());
        let key3: KeySlice = Key::from_slice(&key2);
        if let Some(mut iter) = self.iters.pop() {
            if iter.1.next().is_ok() {
                self.iters.push(iter);
            }
        }
        for x in self.iters.iter() {
            println!("{:?}/{:?}", std::str::from_utf8(x.1.key().raw_ref()), std::str::from_utf8(x.1.value()));
        }
        println!("-----");
        loop {
            let mut stop = false;
            if let Some(mut iter) = self.iters.pop() {
                if iter.1.key() == key3 {
                    if iter.1.next().is_ok() {
                        self.iters.push(iter);
                    }
                } else {
                    stop = true;
                }
            }
            if stop {
                break;
            }
        }
        for x in self.iters.iter() {
            println!("{:?}/{:?}", std::str::from_utf8(x.1.key().raw_ref()), std::str::from_utf8(x.1.value()));
        }
        println!("-----");
        println!("END");
        Ok(())
    }
}
