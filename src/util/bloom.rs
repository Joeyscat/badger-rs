pub(crate) struct Filter(Vec<u8>);

impl Filter {
    pub fn empty() -> Filter {
        Filter(vec![])
    }

    pub fn new(keys: &Vec<u32>, bits_per_key: isize) -> Filter {
        Filter(Self::append_filter(Vec::new(), keys, bits_per_key))
    }

    fn append_filter(buf: Vec<u8>, keys: &Vec<u32>, mut bits_per_key: isize) -> Vec<u8> {
        if bits_per_key < 0 {
            bits_per_key = 0;
        }
        // 0.69 is approximately ln(2).
        let mut k = (bits_per_key as f64 * 0.69) as u32;
        if k < 1 {
            k = 1;
        }
        if k > 30 {
            k = 30;
        }

        let mut n_bits = keys.len() * bits_per_key as usize;
        // For small len(keys), we can see a very high false positive rate. Fix it
        // by enforcing a minimum bloom filter length.
        if n_bits < 64 {
            n_bits = 64;
        }
        let n_bytes = (n_bits + 7) / 8;
        let n_bits = n_bytes * 8;

        let mut c = 1024;
        while c < n_bytes + 1 {
            c += c / 4;
        }
        let mut filter = vec![0; c];

        for mut h in keys.clone() {
            let delta = h >> 17 | h << 15;
            for _ in 0..k {
                let bit_pos = h % n_bits as u32;
                filter[bit_pos as usize / 8] |= 1 << (bit_pos % 8);
                (h, _) = h.overflowing_add(delta);
            }
        }
        filter[n_bytes] = k as u8;
        filter.resize(n_bytes + 1, 0);
        filter
    }

    pub fn bloom(&self) -> &[u8] {
        &self.0
    }
}

impl Filter {
    pub(crate) fn may_contain(bf: &[u8], h: u32) -> bool {
        if bf.len() < 2 {
            return false;
        }
        let k = bf.last().unwrap().clone();
        if k > 30 {
            return true;
        }
        let n_bits = (8 * (bf.len() - 1)) as u32;
        let delta = h >> 17 | h << 15;
        let mut h = h;
        for _ in 0..k {
            let bit_pos = h % n_bits;
            if bf[(bit_pos / 8) as usize] & (1 << (bit_pos % 8)) == 0 {
                return false;
            }
            (h, _) = h.overflowing_add(delta);
        }
        return true;
    }
}

pub fn bloom_bits_per_key(num_entries: isize, fp: f64) -> isize {
    let size = -1.0 * (num_entries) as f64 * fp.ln() / 0.69314718056_f64.powf(2_f64);
    let locs = (0.69314718056_f64 * size / (num_entries as f64)).ceil();
    locs as isize
}

pub fn hash(input: Vec<u8>) -> u32 {
    const SEED: u32 = 0xbc9f1d34;
    const M: u32 = 0xc6a4a793;

    let mut input = input.as_slice();
    let mut h = SEED ^ ((input.len() as u32).overflowing_mul(M)).0;
    loop {
        if input.len() < 4 {
            break;
        }

        (h, _) = h.overflowing_add(
            (input[0] as u32)
                | ((input[1] as u32) << 8)
                | ((input[2] as u32) << 16)
                | ((input[3] as u32) << 24),
        );
        (h, _) = h.overflowing_mul(M);
        h = h ^ (h >> 16);

        input = &input[4..];
    }

    match input.len() {
        3 => {
            h = h + ((input[2] as u32) << 16);
            h = h + ((input[1] as u32) << 8);
            h = h + (input[0] as u32);
            (h, _) = h.overflowing_mul(M);
            h = h ^ (h >> 24);
        }
        2 => {
            h = h + ((input[1] as u32) << 8);
            h = h + (input[0] as u32);
            (h, _) = h.overflowing_mul(M);
            h = h ^ (h >> 24);
        }
        1 => {
            h = h + (input[0] as u32);
            (h, _) = h.overflowing_mul(M);
            h = h ^ (h >> 24);
        }
        0 => {}
        _ => {
            unreachable!("input.len() < 4");
        }
    }

    h
}
#[cfg(test)]
mod tests {

    use crate::util::bloom::hash;

    #[test]
    fn test_hash() {
        let test_cases = vec![
            ("", 0xbc9f1d34),
            ("g", 0xd04a8bda),
            ("go", 0x3e0b0745),
            ("gop", 0x0c326610),
            ("goph", 0x8c9d6390),
            ("gophe", 0x9bfd4b0a),
            ("gopher", 0xa78edc7c),
            ("I had a dream it would end this way.", 0xe14a9db9),
        ];
        for (s, want) in test_cases {
            println!("s: {}, want: {}", s, want);
            let got = hash(s.as_bytes().to_vec());
            assert_eq!(got, want);
        }
    }
}
