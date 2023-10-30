
#![feature(portable_simd)]
#![feature(stdsimd)]
#![feature(is_sorted)]
#![feature(sync_unsafe_cell)]
#![feature(slice_flatten)]
#![feature(ptr_metadata)]
#![feature(once_cell)]

extern crate datafusion;

pub mod utils;
pub mod index;
pub mod optimizer;
pub mod datasources;
pub mod jit;
pub mod query;
pub mod context;
pub mod batch;
pub mod physical_expr;
use jemallocator::Jemalloc;
pub use utils::Result;
pub use context::BooleanContext;
pub use optimizer::{BooleanPhysicalPlanner, MinOperationRange, PartitionPredicateReorder, RewriteBooleanPredicate, PrimitivesCombination};
pub use physical_expr::ShortCircuit;
use clap::{Parser, ValueEnum};

// #[global_allocator]
// static GLOBAL: Jemalloc = Jemalloc;

const JIT_MAX_NODES: usize = 6;

#[derive(Parser, Debug)]
pub struct FastArgs {
    /// file path
    pub path: Vec<String>,

    #[arg(value_enum, long)]
    pub handler: Handler,

    #[arg(short, long)]
    pub partition_num: Option<usize>,

    #[arg(short, long)]
    pub batch_size: Option<u32>,

    #[arg(long)]
    pub base: String,

    #[arg(long, short)]
    pub dump_path: Option<String>,
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