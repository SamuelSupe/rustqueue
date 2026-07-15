#![no_main]

use flate2::read::DeflateDecoder;
use libfuzzer_sys::fuzz_target;
use snap::read::FrameDecoder;
use std::io::Read;

fuzz_target!(|data: &[u8]| {
    if data.len() > 64 * 1024 {
        return;
    }
    let mut output = Vec::new();
    let _ = DeflateDecoder::new(data).take(1024 * 1024).read_to_end(&mut output);
    output.clear();
    let _ = FrameDecoder::new(data).take(1024 * 1024).read_to_end(&mut output);
});
