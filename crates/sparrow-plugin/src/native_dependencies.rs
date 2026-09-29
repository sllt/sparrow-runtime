//! Reject undeclared loader search/code injection before dlopen/exec. System
//! sonames remain an administrator-owned OS baseline, not bundle hash pins.
use crate::invalid;
use sparrow_model::Result;
fn u16_at(bytes: &[u8], at: usize) -> Result<u16> {
    Ok(u16::from_le_bytes(
        bytes
            .get(at..at + 2)
            .ok_or_else(|| invalid("truncated ELF header"))?
            .try_into()
            .unwrap(),
    ))
}
fn u32_at(bytes: &[u8], at: usize) -> Result<u32> {
    Ok(u32::from_le_bytes(
        bytes
            .get(at..at + 4)
            .ok_or_else(|| invalid("truncated ELF header"))?
            .try_into()
            .unwrap(),
    ))
}
fn u64_at(bytes: &[u8], at: usize) -> Result<u64> {
    Ok(u64::from_le_bytes(
        bytes
            .get(at..at + 8)
            .ok_or_else(|| invalid("truncated ELF header"))?
            .try_into()
            .unwrap(),
    ))
}
fn span(bytes: &[u8], offset: u64, size: u64) -> Result<&[u8]> {
    let end = offset
        .checked_add(size)
        .ok_or_else(|| invalid("ELF range overflow"))?;
    if end > bytes.len() as u64 {
        return Err(invalid("ELF segment outside artifact"));
    }
    Ok(&bytes[offset as usize..end as usize])
}
pub(crate) fn validate(bytes: &[u8]) -> Result<()> {
    if bytes.len() < 64 {
        return Err(invalid("truncated ELF64 header"));
    }
    let offset = u64_at(bytes, 32)?;
    let size = u16_at(bytes, 54)? as u64;
    let count = u16_at(bytes, 56)? as u64;
    if size != 56 || count == 0 || count > 128 {
        return Err(invalid("unsupported ELF program header bounds"));
    }
    let headers = span(bytes, offset, size * count)?;
    let mut loads = vec![];
    let mut dynamic = None;
    for h in headers.chunks_exact(56) {
        let kind = u32_at(h, 0)?;
        let offset = u64_at(h, 8)?;
        let address = u64_at(h, 16)?;
        let size = u64_at(h, 32)?;
        if kind == 1 {
            span(bytes, offset, size)?;
            loads.push((offset, address, size));
        }
        if kind == 2 {
            if dynamic.is_some() || size % 16 != 0 || size > 64 * 1024 {
                return Err(invalid("invalid ELF dynamic table"));
            }
            dynamic = Some(span(bytes, offset, size)?);
        }
        if kind == 3 {
            let interpreter = span(bytes, offset, size)?;
            if !matches!(
                interpreter,
                b"/lib64/ld-linux-x86-64.so.2\0" | b"/lib/ld-linux-aarch64.so.1\0"
            ) {
                return Err(invalid("unknown platform ELF interpreter"));
            }
        }
    }
    let Some(dynamic) = dynamic else {
        return Err(invalid("native plugin requires a bounded dynamic ELF"));
    };
    let mut strtab = None;
    let mut strsz = None;
    let mut needed = vec![];
    let mut terminated = false;
    for item in dynamic.chunks_exact(16) {
        let tag = u64_at(item, 0)?;
        let value = u64_at(item, 8)?;
        match tag {
            0 => {
                terminated = true;
                break;
            }
            1 => {
                needed.push(value);
                if needed.len() > 16 {
                    return Err(invalid("too many native platform dependencies"));
                }
            }
            5 => {
                if strtab.replace(value).is_some() {
                    return Err(invalid("duplicate ELF string table"));
                }
            }
            10 => {
                if strsz.replace(value).is_some() {
                    return Err(invalid("duplicate ELF string size"));
                }
            }
            15 | 29 | 0x6ffffefb | 0x6ffffefc | 0x7ffffffd | 0x7fffffff => {
                return Err(invalid(
                    "native RPATH/RUNPATH/audit/filter dependencies are forbidden",
                ))
            }
            _ => {}
        }
    }
    if !terminated {
        return Err(invalid("unterminated ELF dynamic table"));
    }
    if needed.is_empty() {
        return Ok(());
    }
    let (Some(address), Some(size)) = (strtab, strsz) else {
        return Err(invalid("missing ELF dependency string table"));
    };
    let location = loads
        .into_iter()
        .find_map(|(offset, start, len)| {
            let delta = address.checked_sub(start)?;
            (delta.checked_add(size)? <= len)
                .then(|| offset.checked_add(delta))
                .flatten()
        })
        .ok_or_else(|| invalid("ELF string table outside load segments"))?;
    let strings = span(bytes, location, size)?;
    for name in needed {
        let tail = strings
            .get(usize::try_from(name).map_err(|_| invalid("ELF string offset overflow"))?..)
            .ok_or_else(|| invalid("ELF dependency offset outside table"))?;
        let len = tail
            .iter()
            .position(|b| *b == 0)
            .ok_or_else(|| invalid("unterminated ELF dependency"))?;
        if !matches!(
            &tail[..len],
            b"libc.so.6"
                | b"libm.so.6"
                | b"libdl.so.2"
                | b"libpthread.so.0"
                | b"librt.so.1"
                | b"libgcc_s.so.1"
                | b"libstdc++.so.6"
                | b"ld-linux-x86-64.so.2"
                | b"ld-linux-aarch64.so.1"
        ) {
            return Err(invalid("unknown native library dependency; static-link it or use the explicit external-process SDK"));
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn packages_elf_header_ranges_never_read_outside_artifact() {
        for n in 0..256 {
            assert!(validate(&vec![0; n]).is_err());
        }
        let mut bytes = vec![0; 128];
        bytes[32..40].copy_from_slice(&u64::MAX.to_le_bytes());
        bytes[54..56].copy_from_slice(&56u16.to_le_bytes());
        bytes[56..58].copy_from_slice(&1u16.to_le_bytes());
        assert!(validate(&bytes).is_err());
    }
}
