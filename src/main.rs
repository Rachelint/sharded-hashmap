use rand::seq::SliceRandom;
use rand::thread_rng;
use std::collections::HashMap;
use std::time::Instant;

type KeyType = u128;
type ValueType = u128;

#[derive(Copy, Clone, Debug, Default)]
struct KVPair {
    key: KeyType,
    value: ValueType,
}

fn gen_hash_base(len: usize, shard_num: usize) -> Vec<HashMap<KeyType, Vec<ValueType>>> {
    let mut hash_base = vec![HashMap::with_capacity(len / shard_num); shard_num];
    for i in 0..len as KeyType {
        let shard_id = (i % shard_num as KeyType) as usize;
        let mut vec = Vec::with_capacity(16);
        vec.push(1);
        hash_base[shard_id].insert(i, vec);
    }

    hash_base
}

fn gen_vec_delta_buf(len: usize, shard_num: usize) -> Vec<Vec<KVPair>> {
    let per_buf_cap = len / shard_num;
    let buf = vec![Vec::with_capacity(per_buf_cap); shard_num];
    buf
}

fn gen_delta(len: usize) -> Vec<KVPair> {
    let mut vec_delta = Vec::with_capacity(len);
    for i in 0..len as KeyType {
        vec_delta.push(KVPair { key: i, value: 2 });
    }
    vec_delta.shuffle(&mut thread_rng());

    vec_delta
}

fn hash_merge_vec_delta(hash_base: &mut Vec<HashMap<KeyType, Vec<ValueType>>>, vec_buf: &mut Vec<Vec<KVPair>>, delta: &Vec<KVPair>, merge: bool) {
    if !merge {
        let shard_num = hash_base.len();
        for pair in delta {
            let shard_id = (pair.key % shard_num as KeyType) as usize;
            vec_buf[shard_id].push(*pair);
        }
        return;
    }

    for (shard_id, shard_buf) in vec_buf.iter().enumerate() {
        for pair in shard_buf {
            let a = hash_base[shard_id].entry(pair.key).or_default();
            a.push(pair.value);
        }
    }
}

fn hash_merge(hash_base: &mut Vec<HashMap<KeyType, Vec<ValueType>>>, delta: Vec<KVPair>) {
    let shard_num = hash_base.len();
    for KVPair { key, value } in delta {
        let shard_id = (key % shard_num as KeyType) as usize;
        let entry = hash_base[shard_id].entry(key).or_default();
        entry.push(value);
    }
}

fn inspect(maps: &Vec<HashMap<KeyType, Vec<ValueType>>>) {
    for (idx, map) in maps.iter().enumerate() {
        println!("###idx:{idx}, map cap:{}", map.capacity())
    }
}

fn main() {
    let mode = std::env::args().nth(1).unwrap();
    let bench_len = std::env::args().nth(2).unwrap().parse::<usize>().unwrap();
    let shard_num = std::env::args().nth(3).unwrap().parse::<usize>().unwrap();
    let cnt =  std::env::args().nth(4).unwrap().parse::<usize>().unwrap();

    // hash merge
    if mode == "random" {
        let mut append_base = gen_hash_base(bench_len, shard_num);
        let delta = gen_delta(bench_len);
        let timer = Instant::now();
        for _ in 0..cnt {
            hash_merge(&mut append_base, delta.clone());
        }
        let elapsed = timer.elapsed();
        println!("mode:{mode} append cost:{elapsed:?}");
    } else if mode == "buffer" {
        let mut append_base = gen_hash_base(bench_len, shard_num);
        let mut vec_buf = gen_vec_delta_buf(bench_len, shard_num);
        let delta = gen_delta(bench_len);
        let timer = Instant::now();
        for _ in 0..cnt {
            let delta_clone = delta.clone();
            hash_merge_vec_delta(&mut append_base, &mut vec_buf, &delta_clone, false);
            hash_merge_vec_delta(&mut append_base, &mut vec_buf, &delta_clone, true);
            for buf in vec_buf.iter_mut() {
                buf.clear();
            }
        }
        let elapsed = timer.elapsed();
        println!("mode:{mode} append cost:{elapsed:?}");
    } else if mode == "inspect" {
        let append_base = gen_hash_base(bench_len, shard_num);
        inspect(&append_base);
    }
}
