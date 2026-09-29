use sparrow_plugin::{
    script::{ErrorPhase, ScriptFailure, WireValue},
    Manifest, ValueType, MAX_VALUE,
};
use wasmi::{
    Config, Engine, Instance, Memory, Module as WasmModule, Store, StoreLimits, StoreLimitsBuilder,
    TrapCode, TypedFunc,
};
type Result<T> = std::result::Result<T, ScriptFailure>;
const MEMORY_BYTES: usize = 16 * 1024 * 1024;
const FUEL: u64 = 1_000_000;
const BUFFER_BYTES: usize = 256 + MAX_VALUE * 2;
const OUTPUT: usize = 192;
const INPUT_BYTES: usize = 256;
const OUTPUT_BYTES: usize = 256 + MAX_VALUE;
type Call = TypedFunc<(i32, i32, i32, i32, i32, i32), i32>;
fn error(phase: ErrorPhase, reason: &str) -> ScriptFailure {
    ScriptFailure::plain(phase, reason)
}
fn trap(e: wasmi::Error) -> ScriptFailure {
    error(
        ErrorPhase::Call,
        if e.as_trap_code() == Some(TrapCode::OutOfFuel) {
            "wasm fuel"
        } else {
            "WASM trap"
        },
    )
}
pub struct Module {
    pub manifest: Manifest,
    pub source_bytes: usize,
    engine: Engine,
    module: WasmModule,
}
struct Invocation {
    store: Store<StoreLimits>,
    memory: Memory,
    call: Call,
    base: usize,
}
impl Module {
    pub fn new(manifest: Manifest, bytes: &[u8]) -> Result<Self> {
        if !manifest.is_wasm() {
            return Err(error(ErrorPhase::Compile, "WASM kind required"));
        }
        manifest
            .check_artifact(bytes)
            .map_err(|_| error(ErrorPhase::Compile, "WASM artifact rejected"))?;
        let mut config = Config::default();
        config
            .consume_fuel(true)
            .allow_start_fn(false)
            .ignore_custom_sections(true)
            .wasm_multi_memory(false)
            .wasm_custom_page_sizes(false)
            .compilation_mode(wasmi::CompilationMode::Eager)
            .set_max_recursion_depth(128)
            .set_max_stack_height(64 * 1024)
            .set_max_cached_stacks(1);
        let engine = Engine::new(&config);
        let module = WasmModule::new(&engine, bytes)
            .map_err(|_| error(ErrorPhase::Compile, "invalid WASM module"))?;
        if module.imports().next().is_some() {
            return Err(error(ErrorPhase::Compile, "WASM imports forbidden"));
        }
        let this = Self {
            manifest,
            source_bytes: bytes.len(),
            engine,
            module,
        };
        this.instantiate()?;
        Ok(this)
    }
    fn instantiate(&self) -> Result<Invocation> {
        let limits = StoreLimitsBuilder::new()
            .memory_size(MEMORY_BYTES)
            .memories(1)
            .tables(1)
            .table_elements(4096)
            .instances(1)
            .trap_on_grow_failure(true)
            .build();
        let mut store = Store::new(&self.engine, limits);
        store.limiter(|l| l);
        store.set_fuel(FUEL).map_err(trap)?;
        let instance = Instance::new(&mut store, &self.module, &[])
            .map_err(|_| error(ErrorPhase::Initialize, "wasm memory"))?;
        let abi = instance
            .get_typed_func::<(), i32>(&store, "sparrow_wasm_abi_v1")
            .map_err(|_| error(ErrorPhase::Export, "WASM ABI export"))?;
        if abi.call(&mut store, ()).map_err(trap)? != 1 {
            return Err(error(ErrorPhase::Export, "WASM ABI version"));
        }
        let memory = instance
            .get_memory(&store, "memory")
            .ok_or_else(|| error(ErrorPhase::Export, "WASM memory export"))?;
        let buffer = instance
            .get_typed_func::<(), i32>(&store, "sparrow_wasm_buffer_v1")
            .map_err(|_| error(ErrorPhase::Export, "WASM buffer export"))?;
        let base = buffer.call(&mut store, ()).map_err(trap)? as u32 as usize;
        if base % 8 != 0
            || base
                .checked_add(BUFFER_BYTES)
                .is_none_or(|end| end > memory.data_size(&store))
        {
            return Err(error(ErrorPhase::Export, "WASM buffer bounds"));
        }
        let call = instance
            .get_typed_func(&store, "sparrow_wasm_call_v1")
            .map_err(|_| error(ErrorPhase::Export, "WASM call signature"))?;
        Ok(Invocation {
            store,
            memory,
            call,
            base,
        })
    }
    pub fn invoke(&self, id: u32, args: Vec<WireValue>) -> Result<WireValue> {
        let def = self
            .manifest
            .functions
            .iter()
            .find(|d| d.id == id)
            .ok_or_else(|| error(ErrorPhase::Export, "function"))?;
        if args.len() != def.inputs.len() {
            return Err(error(ErrorPhase::Arguments, "arity"));
        }
        let mut descriptors = Vec::with_capacity(8);
        let mut payload = Vec::new();
        let mut total_payload = 0usize;
        let mut null = false;
        for (value, ty) in args.into_iter().zip(&def.inputs) {
            let (tag, bits, data): (u32, u64, Vec<u8>) = match (value, ty) {
                (WireValue::Null, _) => {
                    null = true;
                    (0, 0, vec![])
                }
                (WireValue::Bool(v), ValueType::Bool) => (1, u64::from(v), vec![]),
                (WireValue::Int(v), ValueType::Int64) => (
                    2,
                    v.parse::<i64>()
                        .map_err(|_| error(ErrorPhase::Arguments, "integer"))?
                        as u64,
                    vec![],
                ),
                (WireValue::UInt(v), ValueType::UInt64) => (
                    3,
                    v.parse()
                        .map_err(|_| error(ErrorPhase::Arguments, "integer"))?,
                    vec![],
                ),
                (WireValue::Float(v), ValueType::Float64) if v.is_finite() => {
                    (4, v.to_bits(), vec![])
                }
                (WireValue::Text(v), ValueType::Utf8) => (5, 0, v.into_bytes()),
                (WireValue::Bytes(v), ValueType::Bytes) => (6, 0, v),
                (WireValue::Time(v), ValueType::TimestampMicrosUtc) => (
                    7,
                    v.parse::<i64>()
                        .map_err(|_| error(ErrorPhase::Arguments, "integer"))?
                        as u64,
                    vec![],
                ),
                _ => return Err(error(ErrorPhase::Arguments, "input signature")),
            };
            total_payload = total_payload.saturating_add(match tag {
                0 => 0,
                1 => 1,
                5 | 6 => data.len(),
                _ => 8,
            });
            if total_payload > MAX_VALUE {
                return Err(error(ErrorPhase::Arguments, "input bytes"));
            }
            descriptors.push((tag, bits, payload.len(), data.len()));
            payload.extend_from_slice(&data);
        }
        if null {
            return Ok(WireValue::Null);
        }
        let mut invoke = self.instantiate()?;
        let base = invoke.base;
        let data = invoke.memory.data_mut(&mut invoke.store);
        data[base..base + BUFFER_BYTES].fill(0);
        for (i, (tag, bits, offset, len)) in descriptors.iter().copied().enumerate() {
            let p = base + i * 24;
            data[p..p + 4].copy_from_slice(&tag.to_le_bytes());
            data[p + 4..p + 8].copy_from_slice(&(len as u32).to_le_bytes());
            data[p + 8..p + 16].copy_from_slice(&bits.to_le_bytes());
            let ptr = if tag == 5 || tag == 6 {
                (base + INPUT_BYTES + offset) as u32
            } else {
                0
            };
            data[p + 16..p + 20].copy_from_slice(&ptr.to_le_bytes());
        }
        data[base + INPUT_BYTES..base + INPUT_BYTES + payload.len()].copy_from_slice(&payload);
        let code = invoke
            .call
            .call(
                &mut invoke.store,
                (
                    id as i32,
                    base as i32,
                    descriptors.len() as i32,
                    (base + OUTPUT) as i32,
                    (base + OUTPUT_BYTES) as i32,
                    def.max_output_bytes as i32,
                ),
            )
            .map_err(trap)?;
        if code != 0 {
            return Err(error(ErrorPhase::Call, "WASM function status"));
        }
        let data = invoke.memory.data(&invoke.store);
        let p = base + OUTPUT;
        let tag = u32::from_le_bytes(data[p..p + 4].try_into().unwrap());
        let len = u32::from_le_bytes(data[p + 4..p + 8].try_into().unwrap()) as usize;
        let bits = u64::from_le_bytes(data[p + 8..p + 16].try_into().unwrap());
        let ptr = u32::from_le_bytes(data[p + 16..p + 20].try_into().unwrap()) as usize;
        if data[p + 20..p + 24] != [0; 4] || len > def.max_output_bytes {
            return Err(error(ErrorPhase::Result, "output bytes"));
        }
        if tag == 0 {
            return if bits == 0 && len == 0 && ptr == 0 {
                Ok(WireValue::Null)
            } else {
                Err(error(ErrorPhase::Result, "invalid NULL"))
            };
        }
        if tag != def.output.tag() || ((tag != 5 && tag != 6) && (len != 0 || ptr != 0)) {
            return Err(error(ErrorPhase::Result, "result signature"));
        }
        Ok(match def.output {
            ValueType::Bool if bits <= 1 => WireValue::Bool(bits != 0),
            ValueType::Int64 => WireValue::Int((bits as i64).to_string()),
            ValueType::UInt64 => WireValue::UInt(bits.to_string()),
            ValueType::TimestampMicrosUtc => WireValue::Time((bits as i64).to_string()),
            ValueType::Float64 if f64::from_bits(bits).is_finite() => {
                WireValue::Float(f64::from_bits(bits))
            }
            ValueType::Utf8 | ValueType::Bytes if bits == 0 && ptr == base + OUTPUT_BYTES => {
                let value = &data[ptr..ptr + len];
                if def.output == ValueType::Utf8 {
                    WireValue::Text(
                        std::str::from_utf8(value)
                            .map_err(|_| error(ErrorPhase::Result, "invalid Unicode"))?
                            .to_owned(),
                    )
                } else {
                    WireValue::Bytes(value.to_vec())
                }
            }
            _ => return Err(error(ErrorPhase::Result, "result signature")),
        })
    }
}
