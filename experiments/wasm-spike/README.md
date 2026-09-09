# WASM spike (optional, incomplete)

This crate is an **independent track**. It is **not** part of the V0.4
release criteria and is **not** in workspace `default-members` or
`members`, so `cargo test --workspace` and `cargo build` do not build it.

## Status

- Host `rlib` compiles a leaf `add_i64` / `spike_eval`.
- No Graph/SQL operator offload.
- No sparrow-runtime dependency.
- `wasm32-unknown-unknown` is optional:

```bash
rustup target add wasm32-unknown-unknown
cargo build -p sparrow-wasm-spike --target wasm32-unknown-unknown --manifest-path experiments/wasm-spike/Cargo.toml
```

If the wasm32 target is missing, skip this crate. Do not block V0.4.
