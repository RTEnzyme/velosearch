pub mod utils;
pub mod index;
// pub use utils::parse_wiki_file;

pub use utils::Result;

use clap::{Parser, ValueEnum};

#[derive(Parser, Debug)]
pub struct FastArgs {
    /// file path
    #[arg(short, long)]
    pub path: Option<String>,

    #[arg(value_enum)]
    pub handler: Handler
}

#[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Ord, ValueEnum, Debug)]
pub enum Handler {
   Base,
   SplitBase,
   SplitO1
}