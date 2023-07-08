
#![feature(portable_simd)]
#![feature(stdsimd)]
#![feature(is_sorted)]

extern crate datafusion;

pub mod utils;
pub mod index;
pub mod optimizer;
pub mod datasources;
pub mod jit;
pub mod query;
pub mod context;
pub mod batch;
pub use utils::Result;
pub use context::BooleanContext;
pub use optimizer::{BooleanPhysicalPlanner, IntersectionSelection, MinOperationRange, PartitionPredicateReorder, RewriteBooleanPredicate};

use clap::{Parser, ValueEnum};

#[derive(Parser, Debug)]
pub struct FastArgs {
    /// file path
    pub path: Vec<String>,

    #[arg(value_enum, short)]
    pub handler: Handler,

    #[arg(short, long)]
    pub partition_num: Option<usize>,

    #[arg(short, long)]
    pub batch_size: Option<u32>,

    #[arg(long)]
    pub base: String,
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