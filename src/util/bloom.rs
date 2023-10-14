/// for bloom filter
pub fn hash(b: &Vec<u8>) -> u32 {
    // hash function from leveldb
    let mut seed: u32 = 0xbc9f1d34;
    for x in b {
        seed = seed.wrapping_mul(0x100001b3);
        seed ^= *x as u32;
    }
    seed
}

pub struct Filter(Vec<u8>);

impl Filter {
    pub fn empty() -> Filter {
        Filter(vec![])
    }

    pub fn new(keys: &Vec<u32>, bits_per_key: isize) -> Filter {
        todo!()
    }

    pub fn bloom(self) -> Vec<u8> {
        self.0
    }
}

/// Returns the bits per key required by bloomfilter based on the false positive rate.
pub fn bloom_bits_per_key(num_entries: isize, fp: f64) -> isize {
    let size = (-1 * num_entries) as f64 * fp.log2() / 0.69314718056_f64.powf(2_f64);
    let locs = (0.69314718056_f64 * size / (num_entries as f64)).ceil();
    locs as isize
}

#[cfg(test)]
mod tests {

    /*
        func TestHash(t *testing.T) {
        // The magic want numbers come from running the C++ leveldb code in hash.cc.
        testCases := []struct {
            s    string
            want uint32
        }{
            {"", 0xbc9f1d34},
            {"g", 0xd04a8bda},
            {"go", 0x3e0b0745},
            {"gop", 0x0c326610},
            {"goph", 0x8c9d6390},
            {"gophe", 0x9bfd4b0a},
            {"gopher", 0xa78edc7c},
            {"I had a dream it would end this way.", 0xe14a9db9},
        }
        for _, tc := range testCases {
            if got := Hash([]byte(tc.s)); got != tc.want {
                t.Errorf("s=%q: got 0x%08x, want 0x%08x", tc.s, got, tc.want)
            }
        }
    }
         */

    // use crate::util::bloom::{hash};

    // // 重写上面的go测试用例
    // #[test]
    // fn test_hash() {
    //     let test_cases = vec![
    //         ("", 0xbc9f1d34),
    //         ("g", 0xd04a8bda),
    //         ("go", 0x3e0b0745),
    //         ("gop", 0x0c326610),
    //         ("goph", 0x8c9d6390),
    //         ("gophe", 0x9bfd4b0a),
    //         ("gopher", 0xa78edc7c),
    //         ("I had a dream it would end this way.", 0xe14a9db9),
    //     ];
    //     for (s, want) in test_cases {
    //         let got = hash(&s.as_bytes().to_vec());
    //         assert_eq!(got, want);
    //     }
    // }
}
