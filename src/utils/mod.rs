pub mod json;
pub mod err;

pub use json::parse_wiki_file;
pub use err::{Result, FastErr};
