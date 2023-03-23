pub mod json;
pub mod err;

pub use json::{to_hashmap, parse_wiki_file};
pub use err::{Result, FastErr};
