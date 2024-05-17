use rand::seq::SliceRandom;
use rand::thread_rng;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::mem;
use std::time::Instant;
use voracious_radix_sort::RadixSort;
use voracious_radix_sort::Radixable;

#[derive(Copy, Clone, Debug, Default)]
struct TestPair {
    key: u128,
    value: u128,
}

impl PartialOrd for TestPair {
    fn partial_cmp(&self, other: &TestPair) -> Option<Ordering> {
        self.key.partial_cmp(&other.key)
    }
}

impl PartialEq for TestPair {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Radixable<u128> for TestPair {
    type Key = u128;
    #[inline]
    fn key(&self) -> Self::Key {
        self.key
    }
}

#[derive(Copy, Clone, Debug, Default)]
struct ShardedTestPair {
    shard_id: u32,
    key: u128,
    value: u128,
}

impl PartialOrd for ShardedTestPair {
    fn partial_cmp(&self, other: &ShardedTestPair) -> Option<Ordering> {
        self.shard_id.partial_cmp(&other.shard_id)
    }
}

impl PartialEq for ShardedTestPair {
    fn eq(&self, other: &Self) -> bool {
        self.shard_id == other.shard_id
    }
}

impl Radixable<u32> for ShardedTestPair {
    type Key = u32;
    #[inline]
    fn key(&self) -> Self::Key {
        self.shard_id
    }
}

fn gen_hash_base(len: usize, shard_num: usize) -> Vec<HashMap<u128, Vec<u128>>> {
    let mut hash_base = vec![HashMap::with_capacity(len); shard_num];
    for i in 0..len as u128 {
        let shard_id = (i % shard_num as u128) as usize;
        let mut vec = Vec::with_capacity(16);
        vec.push(1);
        hash_base[shard_id].insert(i, vec);
    }

    hash_base
}

fn gen_sorted_vec_base(len: usize) -> Vec<(u128, Vec<u128>)> {
    let mut vec_base = Vec::with_capacity(len);
    for i in 0..len as u128 {
        let mut vec = Vec::with_capacity(16);
        vec.push(1);
        vec_base.push((i, vec));
    }

    vec_base
}

fn gen_delta(len: usize) -> Vec<(u128, u128)> {
    let mut vec_delta = Vec::with_capacity(len);
    for i in 0..len as u128 {
        for _ in 0..3 {
            vec_delta.push((i, 2));
        }
    }
    vec_delta.shuffle(&mut thread_rng());

    vec_delta
}

fn gen_delta2(len: usize) -> Vec<TestPair> {
    let mut vec_delta = Vec::with_capacity(len);
    for i in 0..len as u128 {
        for _ in 0..2 {
            vec_delta.push(TestPair { key: i, value: 2 });
        }
    }
    vec_delta.shuffle(&mut thread_rng());

    vec_delta
}

fn gen_delta3(len: usize, shard_num: u32) -> Vec<ShardedTestPair> {
    let mut vec_delta = Vec::with_capacity(len);
    for i in 0..len as u128 {
        for _ in 0..2 {
            vec_delta.push(ShardedTestPair {
                key: i,
                value: 2,
                shard_id: i as u32 % shard_num,
            });
        }
    }
    vec_delta.shuffle(&mut thread_rng());

    vec_delta
}

fn hash_merge_batch(hash_base: &mut Vec<HashMap<u128, Vec<u128>>>, mut delta: Vec<ShardedTestPair>) {
    delta.sort_by_key(|k| k.shard_id);
    for d in delta.into_iter() {
        let a = hash_base[d.shard_id as usize].entry(d.key).or_default();
        a.push(d.value);
    }
}

fn hash_merge(hash_base: &mut Vec<HashMap<u128, Vec<u128>>>, mut delta: Vec<TestPair>) {
    let shard_num = hash_base.len();
    for TestPair { key, value } in delta {
        let shard_id = (key % shard_num as u128) as usize;
        let entry = hash_base[shard_id].entry(key).or_default();
        entry.push(value);
    }
}

fn gen_append_vec_base(len: usize, shard_num: usize) -> Vec<Vec<TestPair>> {
    let chunk_size = len / shard_num;
    vec![Vec::with_capacity(chunk_size * 2); shard_num]
}

fn vec_append_batch(mut vec_base: Vec<Vec<TestPair>>, mut delta: Vec<ShardedTestPair>) {
    delta.voracious_sort();
    // let shard_num = vec_base.len();
    // let shard_size = delta.len() / shard_num;
    // let mut buf = vec![Vec::with_capacity(shard_size); shard_num];
    // for ShardedTestPair {
    //     shard_id,
    //     key,
    //     value,
    // } in delta
    // {
    //     buf[shard_id as usize].push(TestPair { key, value });
    // }

    // for (shard, sub_buf) in buf.into_iter().enumerate() {
    //     vec_base[shard].extend(sub_buf);
    // }
}

fn vec_append(mut vec_base: Vec<Vec<TestPair>>, delta: Vec<ShardedTestPair>) {
    for ShardedTestPair {
        key,
        value,
        shard_id,
    } in delta
    {
        vec_base[shard_id as usize].push(TestPair { key, value });
    }
}

fn vec_append_basic(mut vec_base: Vec<TestPair>, delta: Vec<ShardedTestPair>) {
    for pair in delta {
        vec_base.push(TestPair { key: pair.key, value: pair.value })
    }
}

fn sort_merge(mut base: Vec<(u128, Vec<u128>)>, mut delta: Vec<TestPair>) {
    delta.voracious_sort();
    // delta.sort_by_key(|a| a.key);
    let mut miss_deltas = Vec::with_capacity(delta.len() / 10);
    // applying, iters should just be alive during this procedure
    let mut base_iter = base.iter_mut();
    let mut delta_iter = delta.iter_mut();
    let mut base_item_opt = base_iter.next();
    let mut delta_item_opt = delta_iter.next();
    loop {
        let base_item = base_item_opt;
        let delta_item = delta_item_opt;

        if base_item.is_none() || delta_item.is_none() {
            break;
        }

        let base_item = base_item.unwrap();
        let delta_item = delta_item.unwrap();
        match delta_item.key.cmp(&base_item.0) {
            std::cmp::Ordering::Equal => {
                let ts_column = mem::take(&mut delta_item.value);
                base_item.1.push(ts_column);
                delta_item_opt = delta_iter.next();
                base_item_opt = Some(base_item);
            }
            std::cmp::Ordering::Less => {
                let ts_item = mem::take(delta_item);
                miss_deltas.push(ts_item);
                delta_item_opt = delta_iter.next();
                base_item_opt = Some(base_item);
            }
            std::cmp::Ordering::Greater => {
                base_item_opt = base_iter.next();
                delta_item_opt = Some(delta_item);
            }
        }
    }

    // all the rest deltas are misses
    for delta_rest in delta_iter {
        let ts_item = mem::take(delta_rest);
        miss_deltas.push(ts_item);
    }
}

fn main() {
    // let bench_len = 50000;
    // let delta1 = gen_delta2(bench_len);
    // let delta2 = gen_delta2(bench_len);
    // let delta3 = gen_delta3(bench_len, 50000);
    // let vec_base = gen_sorted_vec_base(bench_len);
    // let hash_base1 = gen_hash_base(bench_len, 50000);
    // let hash_base2 = gen_hash_base(bench_len, 50000);

    // let timer = Instant::now();
    // hash_merge_batch(hash_base1, delta3);
    // let elapsed = timer.elapsed();
    // println!("hash merge batch cost:{elapsed:?}");

    // let timer = Instant::now();
    // hash_merge(hash_base2, delta2);
    // let elapsed = timer.elapsed();
    // println!("hash merge cost:{elapsed:?}");

    // let timer = Instant::now();
    // sort_merge(vec_base, delta1);
    // let elapsed = timer.elapsed();
    // println!("sort merge cost:{elapsed:?}");
    let mode = std::env::args().nth(1).unwrap();
    let bench_len = std::env::args().nth(2).unwrap().parse::<usize>().unwrap();
    let shard_num = std::env::args().nth(3).unwrap().parse::<usize>().unwrap();
    let cnt =  std::env::args().nth(4).unwrap().parse::<usize>().unwrap();

    // hash merge
    if mode == "random" {
        let mut append_base = gen_hash_base (bench_len, shard_num);
        let delta = gen_delta2(bench_len);
        let timer = Instant::now();
        for _ in 0..cnt {
            hash_merge(&mut append_base, delta.clone());
        }
        let elapsed = timer.elapsed();
        println!("mode:{mode} append cost:{elapsed:?}");
    }

    // hash merge 2
    if mode == "batch" {
        let mut append_base = gen_hash_base(bench_len, shard_num);
        let delta = gen_delta3(bench_len, shard_num as u32);
        let timer = Instant::now();
        for _ in 0..cnt {
            hash_merge_batch(&mut append_base, delta.clone());
        }
        let elapsed = timer.elapsed();
        println!("mode:{mode} append cost:{elapsed:?}");
    }

        // let append_base2 = gen_append_vec_base(bench_len, 256);
    // let delta2 = gen_delta3(bench_len, 8192);
    // let delta3 = gen_delta3(bench_len, 8192);
    // let append_base3 = Vec::with_capacity(delta3.len());

    // let timer = Instant::now();
    // vec_append_basic(append_base2, delta2);
    // let elapsed = timer.elapsed();
    // println!("point sort append cost:{elapsed:?}");
}
