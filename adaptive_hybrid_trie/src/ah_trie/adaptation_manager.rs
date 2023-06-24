use dashmap::DashMap;


struct AccessStatistics {
    reads: usize,
    history: u8,
    last_epoch: usize,
}

pub(crate) struct AdaptationManager<'a> {
    skip_length: usize,
    epoch: usize,
    sampling_stat: DashMap<&'a str, AccessStatistics>,
}

impl AdaptationManager<'_> {
    pub fn new(skip_length: usize) -> Self {
        Self {
            skip_length,
            epoch: 0,
            sampling_stat: DashMap::new(),
        }
    }
}