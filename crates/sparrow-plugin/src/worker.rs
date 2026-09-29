//! Common child-side framing and process limits. No engine is linked here.
use crate::script::{Request, Response, WIRE_BYTES, WORKER_MEMORY};
use std::io::{Read, Write};
pub type Result<T> = std::result::Result<T, &'static str>;
pub fn read(input: &mut impl Read) -> Result<Request> {
    let mut size = [0; 4];
    input.read_exact(&mut size).map_err(|_| "input closed")?;
    let size = u32::from_be_bytes(size) as usize;
    if size == 0 || size > WIRE_BYTES {
        return Err("frame size");
    }
    let mut bytes = vec![0; size];
    input.read_exact(&mut bytes).map_err(|_| "input closed")?;
    serde_json::from_slice(&bytes).map_err(|_| "request")
}
pub fn write(output: &mut impl Write, response: &Response) -> Result<()> {
    let bytes = serde_json::to_vec(response).map_err(|_| "response")?;
    if bytes.len() > WIRE_BYTES {
        return Err("frame size");
    }
    output
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .map_err(|_| "output closed")?;
    output.write_all(&bytes).map_err(|_| "output closed")?;
    output.flush().map_err(|_| "output closed")
}
pub fn alarm(seconds: u32) {
    #[cfg(unix)]
    unsafe {
        libc::alarm(seconds);
    }
}
#[cfg(all(target_os = "linux", target_env = "gnu"))]
pub fn restrict() -> Result<()> {
    unsafe {
        let mut mask: libc::sigset_t = std::mem::zeroed();
        if libc::sigemptyset(&mut mask) != 0
            || libc::sigaddset(&mut mask, libc::SIGALRM) != 0
            || libc::sigprocmask(libc::SIG_UNBLOCK, &mask, std::ptr::null_mut()) != 0
            || libc::signal(libc::SIGALRM, libc::SIG_DFL) == libc::SIG_ERR
        {
            return Err("watchdog signal");
        }
        if libc::syscall(libc::SYS_close_range, 3u32, u32::MAX, 0u32) != 0
            || libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0
            || libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0) != 0
        {
            return Err("process limits");
        }
        for (resource, cap) in [
            (libc::RLIMIT_AS, WORKER_MEMORY as u64),
            (libc::RLIMIT_CORE, 0),
            (libc::RLIMIT_NOFILE, 16),
            (libc::RLIMIT_STACK, 8 * 1024 * 1024),
        ] {
            let limit = libc::rlimit {
                rlim_cur: cap,
                rlim_max: cap,
            };
            if libc::setrlimit(resource, &limit) != 0 {
                return Err("resource limit");
            }
        }
    }
    Ok(())
}
#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
pub fn restrict() -> Result<()> {
    Err("Linux GNU required")
}
