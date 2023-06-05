
#![feature(stdsimd)]
use std::{slice::Iter, arch::x86_64::{_mm512_mask_compress_epi16, _mm512_mask_compress_epi32, __m512i, _mm512_mask_compressstoreu_epi16}};

use criterion::{Criterion, BenchmarkId, criterion_group, criterion_main};
use rand::seq::IteratorRandom;
use sorted_iter::*;
use sorted_iter::assume::*;
use datafusion::arrow::{array::{BooleanArray, UInt32Array, Array}, compute::{kernels::filter, and, FilterPredicate, FilterBuilder, IterationStrategy}, util::{bit_iterator::BitIndexIterator, bit_chunk_iterator::UnalignedBitChunk}};

const NUM_LANES: usize = 32;

unsafe fn bitset_to_array_aux(
    bitset: u32,
    output: *mut u16,
) {
    let mut output_end = output;
    let mut ids = from_u16x32([
        0, 1, 2, 3, 4, 5, 6, 7, 8,
        9, 10, 11, 12, 13, 14, 15, 16,
        17, 18, 19, 20, 21, 22, 23, 24,
        25, 26, 27, 28, 29, 30, 31, 
    ]);
    _mm512_mask_compressstoreu_epi16(output_end as *mut u8, bitset, ids);
}

fn bitset_to_array(buffer: &[u8], size: usize) -> Vec<u16> {
    let chunk = UnalignedBitChunk::new(buffer, 0, size);
    let mut output: Vec<u16> = Vec::with_capacity(size);
    chunk
        .iter()
        .for_each(|v| {
            let bitset_1 = v as u32;
            unsafe {
                bitset_to_array_aux(bitset_1, output.as_mut_ptr())
            }
            let bitset_2 = (v >> 32) as u32;
            unsafe {
                bitset_to_array_aux(bitset_2, output.as_mut_ptr())
            }
        });
    output
}

#[inline]
fn bitwise_and(b1: &BooleanArray, b2: &BooleanArray) {
    let and_res = and(&b1, &b2).unwrap();
    bitset_to_array(&and_res.data().buffers()[0], b1.len());
}

#[inline]
fn sorted_list_intersection(a1: Iter<u32>, a2: Iter<u32>) {
    let a1_iter = a1.assume_sorted_by_item();
    let a2_iter = a2.assume_sorted_by_item();
    let _res: Vec<u32> = a1_iter.intersection(a2_iter).cloned().collect();
}

fn bench_itersection(c: &mut Criterion) {
    let num: usize = 1024;
    let mut group = c.benchmark_group("bitmap_vs_sorted");
    let v: Vec<u32> = (0..num as u32).collect();
    let mut rng = rand::thread_rng();
    for i in 1..=20 {
        let sel = i as f64 * 0.05;
        let a1 = v.iter().cloned().choose_multiple(&mut rng, (num as f64 * (i as f64 * 0.05)) as usize);
        let a2 = v.iter().cloned().choose_multiple(&mut rng, (num as f64 * (i as f64 * 0.05)) as usize);
        group.bench_with_input(BenchmarkId::new("Sorted_list", format!("{:.2}", sel)), &(&a1, &a2),
            |b, p| b.iter(|| sorted_list_intersection(p.0.iter(), p.1.iter())));
        let mut b1 = vec![false; num];
        let mut b2 = vec![false; num];
        for item in &a1 {
            b1[*item as usize] = true;
        }
        for item in &a2 {
            b2[*item as usize] = true;
        }
        let b1 = BooleanArray::from(b1);
        let b2 = BooleanArray::from(b2);
        group.bench_with_input(BenchmarkId::new("Bitmap", format!("{:.2}", sel)), &(&b1, &b2),
            |b, p| b.iter(|| bitwise_and(p.0, p.1)));
    }
    group.finish();
}

criterion_group!(benches, bench_itersection);
criterion_main!(benches);

const fn from_u16x32(vals: [u16; NUM_LANES]) -> __m512i {
    union U8x64 {
        vector: __m512i,
        vals: [u16; NUM_LANES],
    }
    unsafe { U8x64 { vals }.vector }
}