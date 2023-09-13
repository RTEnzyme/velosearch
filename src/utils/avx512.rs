use std::arch::x86_64::{_mm512_and_epi64, __m512i, _mm512_or_epi64, _mm512_loadu_epi64};

union U64x8 {
    vector: __m512i,
    vals: [u64; 8],
}

pub fn avx512_bitwise_and(lhs: &mut [u64], rhs: &[u64]) {
    let lanes_num = lhs.len() as isize / 8;
    for i in 0..lanes_num {
        unsafe {
            let left = _mm512_loadu_epi64(lhs.as_ptr().offset(8 * i) as *const i64);
            let right = _mm512_loadu_epi64(rhs.as_ptr().offset(8 * i) as *const i64);
            let vector = _mm512_and_epi64(left, right);
            *(lhs[(i as usize *8)..(i as usize*8 + 8)].as_mut_ptr() as *mut [u64; 8]) = U64x8 { vector }.vals;
        }
    }
    for i in (lanes_num as usize * 8)..lhs.len() {
        lhs[i] = unsafe { lhs.get_unchecked(i) & rhs.get_unchecked(i)};
    }
}

pub fn avx512_bitwise_or(lhs: &mut [u64], rhs: &[u64]) {
    let lanes_num = lhs.len() as isize / 8;
    for i in 0..lanes_num {
        unsafe {
            let left = _mm512_loadu_epi64(lhs.as_ptr().offset(8 * i) as *const i64);
            let right = _mm512_loadu_epi64(rhs.as_ptr().offset(8 * i) as *const i64);
            let vector = _mm512_or_epi64(left, right);
            *(lhs[(i as usize *8)..(i as usize*8 + 8)].as_mut_ptr() as *mut [u64; 8]) = U64x8 { vector }.vals;
        }
    }
    for i in (lanes_num as usize * 8)..lhs.len() {
        lhs[i] = unsafe { lhs.get_unchecked(i) & rhs.get_unchecked(i)};
    }
}