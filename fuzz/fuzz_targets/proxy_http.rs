#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut fields = data.splitn(2, |byte| *byte == 0);
    let Some(target) = fields
        .next()
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
    else {
        return;
    };
    let content_length = fields
        .next()
        .and_then(|bytes| std::str::from_utf8(bytes).ok());
    let _ = rustqueue_proxy::parse_forward_metadata(target, content_length, 64 * 1024 * 1024);
});
