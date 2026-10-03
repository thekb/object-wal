use crate::core::wal::Record;
use thiserror::Error;

const MAGIC: &[u8; 8] = b"!OBJWAL!";
const VERSION: u16 = 1;
pub const CHUNK_HEADER_LEN: usize = MAGIC.len() + 2 + 2;
pub const CHUNK_CHECKSUM_LEN: usize = 4;
pub const CHUNK_OVERHEAD_LEN: usize = CHUNK_HEADER_LEN + CHUNK_CHECKSUM_LEN;
pub const MAX_NUM_RECORDS: usize = u16::MAX as usize;
const MAX_KEY_LEN: usize = 64 * 1024 / 8;
pub const MAX_VAL_LEN: usize = 4 * 1024 * 1024;
pub const MAX_KEY_VALUE_LEN: usize = 64 * 1024 * 1024;
pub const MAX_CHUNK_LEN: usize = MAX_KEY_VALUE_LEN + CHUNK_OVERHEAD_LEN + 8 * MAX_NUM_RECORDS;

#[derive(Error, Debug)]
pub enum CodecError {
    #[error("no records")]
    NoRecords,
    #[error("field overflowed")]
    OverFlow,
    #[error("bad segment")]
    BadSegment,
    #[error("segment checksum mismatch")]
    ChecksumMismatch,
    #[error("left over input")]
    LeftOverInput,
}

pub fn records_size(records: &Vec<Record>) -> Result<usize, CodecError> {
    let mut size: usize = 0;
    for record in records {
        let key_len: usize = checked_len(record.key.len(), MAX_KEY_LEN)?;
        let val_len = checked_len(record.value.len(), MAX_VAL_LEN)?;
        size = size
            .checked_add(8 + key_len + val_len)
            .ok_or(CodecError::OverFlow)?;
    }
    Ok(size)
}

/// Sum of encoded key and value bytes, excluding length fields and chunk framing.
pub fn chunk_payload_size(records: &Vec<Record>) -> Result<usize, CodecError> {
    if records.is_empty() {
        return Err(CodecError::NoRecords);
    }
    checked_len(records.len(), MAX_NUM_RECORDS)?;
    let mut payload = 0usize;
    for record in records {
        let key_len = checked_len(record.key.len(), MAX_KEY_LEN)?;
        let value_len = checked_len(record.value.len(), MAX_VAL_LEN)?;
        payload = payload
            .checked_add(key_len)
            .and_then(|n| n.checked_add(value_len))
            .ok_or(CodecError::OverFlow)?;
        checked_len(payload, MAX_KEY_VALUE_LEN)?;
    }
    Ok(payload)
}

pub fn chunk_size(records: &Vec<Record>) -> Result<usize, CodecError> {
    chunk_payload_size(records)?;
    CHUNK_OVERHEAD_LEN
        .checked_add(records_size(records)?)
        .ok_or(CodecError::OverFlow)
}

/// encode_records encodes a chunk of records to binary representation.
/// Format `!OBJWAL! (8 bytes)|VERSION (2 bytes u16)|NUM RECORDS (2 bytes u16)|<KEY LEN>(4 bytes u32)<KEY><VALUE LEN>(4 bytes u32)<VALUE> x NUM RECORDS|CRC32 (4 bytes, little-endian)`.
/// All integers are little-endian. The CRC32 covers every preceding byte.
pub fn encode_records(records: &Vec<Record>) -> Result<Vec<u8>, CodecError> {
    let total_size = chunk_size(records)?;

    let mut encoded: Vec<u8> = Vec::with_capacity(total_size);
    encoded.extend_from_slice(MAGIC);
    encoded.extend_from_slice(&VERSION.to_le_bytes());
    let num_records = checked_len(records.len(), MAX_NUM_RECORDS)?;
    encoded.extend_from_slice(&(num_records as u16).to_le_bytes());
    for record in records {
        let key_len: usize = checked_len(record.key.len(), MAX_KEY_LEN)?;
        encoded.extend_from_slice(&(key_len as u32).to_le_bytes());
        encoded.extend_from_slice(&record.key);
        let val_len = checked_len(record.value.len(), MAX_VAL_LEN)?;
        encoded.extend_from_slice(&(val_len as u32).to_le_bytes());
        encoded.extend_from_slice(&record.value);
    }
    let checksum = crc32fast::hash(&encoded);
    encoded.extend_from_slice(&checksum.to_le_bytes());
    Ok(encoded)
}

fn checked_len(len: usize, max_val: usize) -> Result<usize, CodecError> {
    if len > u32::MAX as usize || len > max_val {
        return Err(CodecError::OverFlow);
    }
    Ok(len)
}

struct Cursor<'a> {
    data: &'a [u8],
    curr_pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            data: data,
            curr_pos: 0,
        }
    }

    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.curr_pos)
    }

    fn expect(&mut self, expected: &[u8]) -> Result<(), CodecError> {
        let actual = self.read(expected.len())?;
        if actual.ne(expected) {
            return Err(CodecError::BadSegment);
        }
        Ok(())
    }

    fn u16(&mut self) -> Result<u16, CodecError> {
        let bytes: [u8; 2] = self.read(2)?.try_into().map_err(|_| CodecError::OverFlow)?;
        Ok(u16::from_le_bytes(bytes))
    }

    fn u32(&mut self) -> Result<u32, CodecError> {
        let bytes: [u8; 4] = self.read(4)?.try_into().map_err(|_| CodecError::OverFlow)?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read(&mut self, length: usize) -> Result<&'a [u8], CodecError> {
        let end = self
            .curr_pos
            .checked_add(length)
            .ok_or(CodecError::OverFlow)?;

        if end > self.data.len() {
            return Err(CodecError::OverFlow);
        }

        let chunk = &self.data[self.curr_pos..end];
        self.curr_pos = end;
        Ok(chunk)
    }
}

pub fn decode_chunk(data: &[u8]) -> Result<Vec<Record>, CodecError> {
    checked_len(data.len(), MAX_CHUNK_LEN)?;
    if !data.starts_with(MAGIC) {
        return Err(CodecError::BadSegment);
    }
    if data.len() < CHUNK_OVERHEAD_LEN {
        return Err(CodecError::OverFlow);
    }
    let (payload, checksum) = data.split_at(data.len() - CHUNK_CHECKSUM_LEN);
    let expected = u32::from_le_bytes(checksum.try_into().unwrap());
    if crc32fast::hash(payload) != expected {
        return Err(CodecError::ChecksumMismatch);
    }
    let mut cursor = Cursor::new(payload);
    cursor.expect(MAGIC)?;
    if cursor.u16()? != VERSION {
        return Err(CodecError::BadSegment);
    }
    // read number of records
    let num_records = cursor.u16()? as usize;
    checked_len(num_records, MAX_NUM_RECORDS)?;
    if num_records == 0 {
        return Err(CodecError::NoRecords);
    }
    let mut records: Vec<Record> = Vec::with_capacity(num_records);
    // read records
    for _ in 0..num_records {
        let key_len = cursor.u32()?;
        checked_len(key_len as usize, MAX_KEY_LEN)?;
        let key = cursor.read(key_len as usize)?;
        let value_len = cursor.u32()?;
        checked_len(value_len as usize, MAX_VAL_LEN)?;
        let value = cursor.read(value_len as usize)?;
        records.push(Record::new(key.to_vec(), value.to_vec()));
    }

    let key_value_len = records.iter().try_fold(0usize, |sum, record| {
        sum.checked_add(record.key.len())
            .and_then(|n| n.checked_add(record.value.len()))
            .ok_or(CodecError::OverFlow)
    })?;
    if key_value_len > MAX_KEY_VALUE_LEN {
        return Err(CodecError::OverFlow);
    }

    if cursor.remaining() != 0 {
        return Err(CodecError::LeftOverInput);
    }

    Ok(records)
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::core::wal::Record;

    #[test]
    fn test_codec_round_trip() -> Result<(), String> {
        let records = vec![Record::new("somekey", "somevalue")];

        let encoded = encode_records(&records).map_err(|err| err.to_string())?;
        let decoded_chunk = decode_chunk(&encoded).map_err(|err| err.to_string())?;
        if decoded_chunk.ne(&records) {
            return Err(format!("encoded chunk is not equal to decoded chunk"));
        }

        Ok(())
    }

    #[test]
    fn detects_corruption_of_records_and_checksum() {
        let records = vec![Record::new("key", "value")];
        let encoded = encode_records(&records).unwrap();
        assert!(encoded.starts_with(MAGIC));
        assert_eq!(
            &encoded[MAGIC.len()..MAGIC.len() + 2],
            &VERSION.to_le_bytes()
        );
        assert_eq!(
            encoded.len(),
            CHUNK_OVERHEAD_LEN + records_size(&records).unwrap()
        );

        for index in [CHUNK_HEADER_LEN + 4, encoded.len() - 1] {
            let mut corrupt = encoded.clone();
            corrupt[index] ^= 1;
            assert!(matches!(
                decode_chunk(&corrupt),
                Err(CodecError::ChecksumMismatch)
            ));
        }
        let mut truncated = encoded;
        truncated.pop();
        assert!(matches!(
            decode_chunk(&truncated),
            Err(CodecError::ChecksumMismatch)
        ));
    }

    #[test]
    fn rejects_unknown_version() {
        let mut encoded = encode_records(&vec![Record::new("key", "value")]).unwrap();
        encoded[MAGIC.len()..MAGIC.len() + 2].copy_from_slice(&2u16.to_le_bytes());
        let checksum = crc32fast::hash(&encoded[..encoded.len() - CHUNK_CHECKSUM_LEN]);
        let trailer = encoded.len() - CHUNK_CHECKSUM_LEN;
        encoded[trailer..].copy_from_slice(&checksum.to_le_bytes());
        assert!(matches!(
            decode_chunk(&encoded),
            Err(CodecError::BadSegment)
        ));
    }
}
