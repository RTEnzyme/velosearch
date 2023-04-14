use std::collections::HashMap;

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

pub struct TermIdx<T> {
    term_map: HashMap<String, T>
}

impl<T> TermIdx<T> {
    pub fn new() -> Self {
        Self { term_map: HashMap::new() }
    }

    pub fn get(&self, name: &str) -> Option<&T> {
        self.term_map.get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
