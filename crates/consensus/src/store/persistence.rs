use super::*;
use bincode::Options;
use std::io::Read;

const MAX_BINARY_STATE_BYTES: u64 = 512 * 1024 * 1024;

pub(super) fn read_json_optional<T: DeserializeOwned>(path: &Path) -> io::Result<Option<T>> {
    match File::open(path) {
        Ok(file) => serde_json::from_reader(file)
            .map(Some)
            .map_err(io::Error::other),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

pub(super) fn write_json_atomic(path: &Path, value: &impl Serialize) -> io::Result<()> {
    let parent = path.parent().expect("state file has parent");
    fs::create_dir_all(parent)?;
    let temporary = path.with_extension("tmp");
    let mut file = File::create(&temporary)?;
    serde_json::to_writer(&mut file, value).map_err(io::Error::other)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    File::open(parent)?.sync_all()
}

pub(super) fn read_binary_optional<T: DeserializeOwned>(path: &Path) -> io::Result<Option<T>> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if file.metadata()?.len() > MAX_BINARY_STATE_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "binary state exceeds configured safety limit",
        ));
    }
    let value = binary_codec()
        .deserialize_from(&mut file)
        .map_err(io::Error::other)?;
    let mut trailing = [0u8; 1];
    if file.read(&mut trailing)? != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "binary state contains trailing bytes",
        ));
    }
    Ok(Some(value))
}

pub(super) fn write_binary_atomic(path: &Path, value: &impl Serialize) -> io::Result<()> {
    let parent = path.parent().expect("state file has parent");
    fs::create_dir_all(parent)?;
    let temporary = path.with_extension("tmp");
    let mut file = File::create(&temporary)?;
    binary_codec()
        .serialize_into(&mut file, value)
        .map_err(io::Error::other)?;
    if file.metadata()?.len() > MAX_BINARY_STATE_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "binary state exceeds configured safety limit",
        ));
    }
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    File::open(parent)?.sync_all()
}

fn binary_codec() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(MAX_BINARY_STATE_BYTES)
}
