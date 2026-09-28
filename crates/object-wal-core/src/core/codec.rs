use crate::core::wal::{Chunk, Record};
use thiserror::Error;

const MAGIC: &[u8; 16] = b"!OBJECTSTOREWAL!";
const MAX_NUM_RECORDS: usize = 512;
const MAX_KEY_LEN: usize = 64 * 1024 / 8;
const MAX_VAL_LEN: usize = 2 * 1024 * 1024 / 8;
pub const MAX_CHUNK_LEN: usize = 8 * 1024 * 1024 / 8;

#[derive(Error, Debug)]
pub enum CodecError {
    #[error("no records")]
    NoRecords,
    #[error("field overflowed")]
    OverFlow,
    #[error("bad segment")]
    BadSegment,
    #[error("left over input")]
    LeftOverInput,
}

pub fn records_size(records: &Vec<Record>) -> Result<usize, CodecError> {
    let mut size = 0;
    for record in records {
        let key_len: usize = checked_len(record.key.len(), MAX_KEY_LEN)?;
        let val_len = checked_len(record.value.len(), MAX_VAL_LEN)?;
        size += key_len + val_len;
    }
    Ok(size)
}

pub fn chunk_size(records: &Vec<Record>) -> Result<usize, CodecError> {
    if records.is_empty() {
        return Err(CodecError::NoRecords);
    }
    checked_len(records.len(), MAX_NUM_RECORDS)?;
    let mut size: usize = 0;
    size += MAGIC.len(); // header
    size += 2; // num records
    let records_size = records_size(records)?;
    size += records_size;
    return Ok(size);
}

/// encode_records encodes a chunk of records to binary representation.
/// Format `MAGIC (16 bytes [u8, 16])|<NUM RECORDS> (4 bytes u32)|<KEY LEN>(4 bytes u32)<KEY><VALUE LEN>(4 bytes u32)<VALUE> x NUM RECORDS`
pub fn encode_records(records: &Vec<Record>) -> Result<Vec<u8>, CodecError> {
    let total_size = chunk_size(records)?;

    let mut encoded: Vec<u8> = Vec::with_capacity(total_size);
    encoded.extend_from_slice(MAGIC);
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
    let mut cursor = Cursor::new(data);

    // check for MAGIC bytes in the beginning
    cursor.expect(MAGIC)?;
    // read number of records
    let num_records = cursor.u16()? as usize;
    let mut records: Vec<Record> = Vec::with_capacity(num_records);
    // read records
    for _ in 0..num_records {
        let key_len = cursor.u32()?;
        let key = cursor.read(key_len as usize)?;
        let value_len = cursor.u32()?;
        let value = cursor.read(value_len as usize)?;
        records.push(Record::new(key.to_vec(), value.to_vec()));
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
}
