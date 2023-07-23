use anyhow::{Result, Ok};
use memmap::Mmap;
use std::{
    io::{Read, Write},
};

pub(crate) fn encode_varint_to_vec(mut v: u64) -> Result<Vec<u8>> {
    if v == 0 {
        return Ok(vec![0]);
    }
    let mut result: Vec<u8> = Vec::new();
    while v > 0 {
        let mut b = (v & 0x7f) as u8;
        v >>= 7;
        if v > 0 {
            b |= 0x80;
        }
        result.push(b);
    }
    Ok(result)
}

pub(crate) fn encode_varint<W: Write>(v: u64, w: &mut W) -> Result<()> {
    let vector = encode_varint_to_vec(v)?;
    w.write_all(vector.as_slice())?;
    Ok(())
}

pub(crate) fn decode_varint<R: Read>(r: &mut R) -> Result<(u64, u64)> {
    let mut result: u64 = 0;
    let mut shift: u64 = 0;
    let mut buf: [u8; 1] = [0];
    let mut read = 0;
    loop {
        read += r.read(&mut buf)?;
        let byte = buf[0] as u64;
        result |= (byte & 0x7f) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            break;
        }
    }
    Ok((result, read as u64))
}

pub(crate) fn decode_varint_from_mmap(slice: &Mmap, i: &mut usize) -> Result<u64> {
    let mut result: u64 = 0;
    let mut shift: u64 = 0;
    loop {
        let byte = slice[*i] as u64;
        (*i) += 1;
        result |= (byte & 0x7f) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            break;
        }
    }
    Ok(result)
}
