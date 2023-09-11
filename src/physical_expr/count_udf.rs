use std::arch::x86_64::{_mm512_setzero_si512, _mm512_loadu_si512, _mm512_popcnt_epi64, _mm512_add_epi64, _mm512_reduce_add_epi64};

use datafusion::{physical_plan::Accumulator, error::Result, scalar::ScalarValue, arrow::{array::{ArrayRef, Int64Array, as_boolean_array}, compute::sum}, common::{downcast_value, DataFusionError}};
use tracing::{debug, info};


/// Count
#[derive(Debug)]
pub struct CountValid {
    count: i64
}

impl CountValid {
    /// new count accumulator
    pub fn new() -> Self {
        Self { count: 0 }
    }
}

impl Accumulator for CountValid {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Int64(Some(self.count))])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0];
        let count2 = avx512_vpopcnt(array.data().buffers()[0].as_slice()) as i64;
        debug!("simd count: {:}", count2);
        let count = as_boolean_array(&array).true_count() as i64;
        debug!("update count: {:?}", count);
        self.count += count;
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0];
        self.count -= avx512_vpopcnt(array.data().buffers()[0].as_slice()) as i64;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        debug!("merge_batch: {:?}", states);
        let counts = downcast_value!(states[0], Int64Array);
        let delta = &sum(counts);
        if let Some(d) = delta {
            self.count += *d;
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.count)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

fn avx512_vpopcnt(data: &[u8]) -> u64 {
    const CHUNK_SIZE: usize = 64;
    // Should be the multiply of CHUNK_SIZE
    assert!(data.len() % CHUNK_SIZE == 0, "data len: {:}", data.len());
    let chunks = data.len() / CHUNK_SIZE;

    let ptr = data.as_ptr();

    // Count using AVX512 registers
    let mut accumulator = unsafe { _mm512_setzero_si512() };
    for i in 0..chunks {
        unsafe {
            let v = _mm512_loadu_si512(ptr.offset(64 * i as isize) as * const i32);
            let p = _mm512_popcnt_epi64(v);

            accumulator = _mm512_add_epi64(accumulator, p);
        }
    }

    // horizontal sum of a register
    unsafe { _mm512_reduce_add_epi64(accumulator) as u64 }
}
