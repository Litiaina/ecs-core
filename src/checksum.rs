use crc32fast::Hasher as Crc32Hasher;

pub const SHA256_DIGEST_BYTES: usize = 32;
pub type Sha256Digest = [u8; SHA256_DIGEST_BYTES];

/// Checksum algorithm applied to the source and each encoded piece.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChecksumAlgorithm {
    Crc32,
    Sha256,
}

/// Checksum behavior selected once when a stream is initialized.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChecksumPolicy {
    pub algorithm: ChecksumAlgorithm,
    pub source_checksum: bool,
    pub block_checksums: bool,
}

impl ChecksumPolicy {
    #[must_use]
    pub const fn crc32(source_checksum: bool) -> Self {
        Self {
            algorithm: ChecksumAlgorithm::Crc32,
            source_checksum,
            block_checksums: false,
        }
    }

    #[must_use]
    pub const fn sha256(source_checksum: bool) -> Self {
        Self {
            algorithm: ChecksumAlgorithm::Sha256,
            source_checksum,
            block_checksums: false,
        }
    }

    #[must_use]
    pub const fn sha256_with_blocks(source_checksum: bool) -> Self {
        Self {
            algorithm: ChecksumAlgorithm::Sha256,
            source_checksum,
            block_checksums: true,
        }
    }
}

/// Canonical checksum value returned without losing its algorithm identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Digest {
    Crc32(u32),
    Sha256(Sha256Digest),
}

impl Digest {
    #[must_use]
    pub fn to_hex(&self) -> String {
        match self {
            Self::Crc32(value) => format!("{value:08x}"),
            Self::Sha256(value) => hex::encode(value),
        }
    }

    #[must_use]
    pub fn algorithm(&self) -> ChecksumAlgorithm {
        match self {
            Self::Crc32(_) => ChecksumAlgorithm::Crc32,
            Self::Sha256(_) => ChecksumAlgorithm::Sha256,
        }
    }
}

#[derive(Clone)]
// SHA-256 is the hot production policy. Keeping its context inline avoids one
// heap allocation and pointer chase per piece solely to shrink the CRC variant.
#[allow(clippy::large_enum_variant)]
pub(crate) enum StreamHasher {
    Crc32(Crc32Hasher),
    Sha256(ring::digest::Context),
}

impl StreamHasher {
    pub(crate) fn new(algorithm: ChecksumAlgorithm) -> Self {
        match algorithm {
            ChecksumAlgorithm::Crc32 => Self::Crc32(Crc32Hasher::new()),
            ChecksumAlgorithm::Sha256 => {
                Self::Sha256(ring::digest::Context::new(&ring::digest::SHA256))
            }
        }
    }

    pub(crate) fn update(&mut self, bytes: &[u8]) {
        match self {
            Self::Crc32(hasher) => hasher.update(bytes),
            Self::Sha256(hasher) => hasher.update(bytes),
        }
    }

    pub(crate) fn finalize(self) -> Digest {
        match self {
            Self::Crc32(hasher) => Digest::Crc32(hasher.finalize()),
            Self::Sha256(hasher) => Digest::Sha256(
                hasher
                    .finish()
                    .as_ref()
                    .try_into()
                    .expect("SHA-256 is always 32 bytes"),
            ),
        }
    }
}

pub(crate) fn digest_bytes(algorithm: ChecksumAlgorithm, bytes: &[u8]) -> Digest {
    match algorithm {
        ChecksumAlgorithm::Crc32 => {
            let mut hasher = Crc32Hasher::new();
            hasher.update(bytes);
            Digest::Crc32(hasher.finalize())
        }
        ChecksumAlgorithm::Sha256 => Digest::Sha256(
            ring::digest::digest(&ring::digest::SHA256, bytes)
                .as_ref()
                .try_into()
                .expect("SHA-256 is always 32 bytes"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_is_canonical() {
        assert_eq!(
            digest_bytes(ChecksumAlgorithm::Sha256, b"abc").to_hex(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn crc32_is_canonical() {
        assert_eq!(
            digest_bytes(ChecksumAlgorithm::Crc32, b"123456789").to_hex(),
            "cbf43926"
        );
    }
}
