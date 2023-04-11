
#![feature(portable_simd)]
#![feature(stdsimd)]
extern crate datafusion;

pub mod utils;
pub mod index;
pub mod datasources;
pub mod query;
pub mod context;
pub mod batch;
pub use utils::Result;
pub use context::BooleanContext;

use clap::{Parser, ValueEnum};

#[derive(Parser, Debug)]
pub struct FastArgs {
    /// file path
    #[arg(short, long)]
    pub path: Option<String>,

    #[arg(value_enum)]
    pub handler: Handler,

}

#[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Ord, ValueEnum, Debug)]
pub enum Handler {
   Base,
   SplitBase,
   SplitO1,
   LoadData,
   BooleanQuery,
}