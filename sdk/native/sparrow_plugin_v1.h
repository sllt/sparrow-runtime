#ifndef SPARROW_PLUGIN_V1_H
#define SPARROW_PLUGIN_V1_H
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif
/* Linux ELF64 GNU, ABI v1. All structs use native C layout, not packed layout.
 * No pointers may be retained. Inputs are read-only for the duration of call.
 * Output bytes MUST be written only to the supplied host buffer. No allocation
 * or free crosses the boundary. No panic/exception/longjmp may unwind into host.
 * Functions must be pure, thread-safe, promptly returning and bounded. This
 * cooperative contract is NOT enforced against malicious native machine code.
 * Tags: 0 NULL, 1 bool, 2 i64, 3 u64, 4 finite f64 bits, 5 UTF8, 6 bytes, 7 ts-us.
 * Reserved fields are zero. Scalar output length is zero. NULL bits/length zero.
 */
typedef struct {uint32_t tag,reserved;uint64_t bits;const uint8_t *data;uint64_t len;} SparrowInputV1;
typedef struct {uint32_t tag,reserved;uint64_t bits,len;} SparrowOutputV1;
uint32_t sparrow_plugin_abi_v1(void);
int32_t sparrow_plugin_call_v1(uint32_t function,const SparrowInputV1 *input,uint32_t count,
    SparrowOutputV1 *output,uint8_t *buffer,uint64_t capacity);
#ifdef __cplusplus
}
#endif
#endif
