
#![feature(portable_simd)]
#![feature(stdsimd)]
#![feature(is_sorted)]

extern crate datafusion;

pub mod utils;
pub mod index;
pub mod optimizer;
pub mod datasources;
pub mod query;
pub mod context;
pub mod batch;
pub use utils::Result;
pub use context::BooleanContext;
pub use optimizer::{BooleanPhysicalPlanner, IntersectionSelection, MinOperationRange, PartitionPredicateReorder};

use clap::{Parser, ValueEnum};

#[derive(Parser, Debug)]
pub struct FastArgs {
    /// file path
    #[arg(short, long)]
    pub path: Option<String>,

    #[arg(value_enum)]
    pub handler: Handler,

    #[arg(long)]
    pub partition_num: Option<usize>,

    #[arg(short, long)]
    pub batch_size: Option<u32>,
}

#[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Ord, ValueEnum, Debug)]
pub enum Handler {
   Base,
   SplitBase,
   SplitO1,
   LoadData,
   BooleanQuery,
   PostingTable,
   Tantivy,
}