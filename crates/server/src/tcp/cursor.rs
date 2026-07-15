pub(super) fn partition_cursor_seed(sequence: u64, partitions: usize) -> usize {
    if partitions <= 1 {
        return 0;
    }
    let reversed = sequence.reverse_bits() as u128;
    ((reversed * partitions as u128) >> u64::BITS) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequential_connections_are_spread_across_partitions() {
        let mut cursors: Vec<_> = (0..32)
            .map(|offset| partition_cursor_seed(123_456 + offset, 1024))
            .collect();
        cursors.sort_unstable();
        cursors.push(cursors[0] + 1024);
        let largest_gap = cursors
            .windows(2)
            .map(|pair| pair[1] - pair[0])
            .max()
            .unwrap();
        assert!(largest_gap <= 64, "largest cursor gap was {largest_gap}");
    }
}
