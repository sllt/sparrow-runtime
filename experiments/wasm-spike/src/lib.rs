//! Tiny WASM spike. Host-compilable; `wasm32-unknown-unknown` is optional.
//!
//! This is **not** production operator offload and is **not** a V0.4
//! release criterion. Keep it off the default workspace build.

/// Wrapping add so a future wasm32 target can call a leaf kernel.
pub fn add_i64(a: i64, b: i64) -> i64 {
    a.wrapping_add(b)
}

/// Placeholder "eval" that only handles i64 add. Not a binder.
pub fn spike_eval(op: &str, a: i64, b: i64) -> Option<i64> {
    match op {
        "add" => Some(add_i64(a, b)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_wraps() {
        assert_eq!(add_i64(2, 3), 5);
        assert_eq!(spike_eval("add", 1, 2), Some(3));
        assert_eq!(spike_eval("mul", 1, 2), None);
    }
}
