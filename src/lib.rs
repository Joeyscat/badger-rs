#![feature(slice_as_chunks)]

pub mod db;
pub mod error;
pub mod iterator;
pub mod option;
pub mod txn;

mod entry;
mod fb;
mod level;
mod manifest;
mod memtable;
mod read;
mod skiplist;
mod table;
#[cfg(test)]
mod test;
mod util;
mod value;
mod vlog;
mod write;

mod pb {
    include!(concat!(env!("OUT_DIR"), "/badgerpb4.rs"));
}
