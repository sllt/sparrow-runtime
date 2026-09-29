#ifndef SPARROW_WASM_V1_H
#define SPARROW_WASM_V1_H
#include <stdint.h>
/* wasm32 little-endian only; no WASI/imports or start function. Export memory.
 * Descriptor is exactly 24 bytes, aligned to 8. Tags match the scalar ABI:
 * 0=NULL 1=bool 2=i64 3=u64 4=finite-f64-bits 5=UTF8 6=bytes 7=timestamp-us.
 * Scalar len/offset/reserved=0. Variable bytes bits/reserved=0.
 * Every call gets a new instance: globals/memory must not carry session state.
 * Buffer returns an 8-aligned, already allocated arena >=131328 bytes.
 * Host owns input descriptors/payload and supplies output descriptor/payload.
 * Output offset must equal the supplied buffer, len<=capacity. Never retain it.
 * Return 0 only after the complete valid output is written; other codes fail.
 */
typedef struct { uint32_t tag,len; uint64_t bits; uint32_t offset,reserved; } SparrowWasmScalarV1;
#define SPARROW_WASM_BUFFER_BYTES (256u+65536u*2u)
uint32_t sparrow_wasm_abi_v1(void);
uint32_t sparrow_wasm_buffer_v1(void);
int32_t sparrow_wasm_call_v1(uint32_t function,uint32_t input,uint32_t count,
    uint32_t output,uint32_t buffer,uint32_t capacity);
#endif
