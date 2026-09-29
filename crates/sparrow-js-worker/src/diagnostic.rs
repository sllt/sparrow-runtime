//! Only bounded numeric source coordinates cross IPC. No exception message,
//! filenames, function names or business values are exposed.
use rquickjs::{function::This, qjs, Ctx, Function, Value};
use sparrow_plugin::script::{ErrorFrame, ErrorPhase, ScriptFailure, MAX_ERROR_FRAMES};

pub struct Diagnostics<'source, 'js> {
    // Capture the original QuickJS Error.prototype.stack C getter before user
    // code. It returns [[ErrorData]] directly, without looking up user properties.
    stack_getter: Function<'js>,
    source: &'source str,
}
impl<'source, 'js> Diagnostics<'source, 'js> {
    pub fn new(stack_getter: Function<'js>, source: &'source str) -> Self {
        Self {
            stack_getter,
            source,
        }
    }
    pub fn capture(&self, ctx: &Ctx<'js>, phase: ErrorPhase, reason: &str) -> ScriptFailure {
        let mut result = ScriptFailure::plain(phase, reason);
        let exception = ctx.catch();
        // A direct class test: proxies and arbitrary thrown objects are NOT
        // inspected. Never call their getters, toString, or descriptor traps.
        if !unsafe { qjs::JS_IsError(exception.as_raw()) } {
            return result;
        }
        let Ok(value) = self.stack_getter.call::<_, Value>((This(exception),)) else {
            let _ = ctx.catch();
            return result;
        };
        if !value.is_string() {
            return result;
        }
        let mut len = 0;
        let ptr = unsafe { qjs::JS_ToCStringLen(ctx.as_raw().as_ptr(), &mut len, value.as_raw()) };
        if ptr.is_null() {
            let _ = ctx.catch();
            return result;
        }
        if len as usize <= 8192 {
            // A primitive string conversion cannot invoke JS. Invalid UTF-8
            // simply has no diagnostic; do not use unchecked Rust string APIs.
            let bytes = unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), len as usize) };
            if let Ok(stack) = std::str::from_utf8(bytes) {
                result.frames = frames(stack, self.source);
            }
        }
        unsafe {
            qjs::JS_FreeCString(ctx.as_raw().as_ptr(), ptr);
        }
        result
    }
}
fn frames(stack: &str, source: &str) -> Vec<ErrorFrame> {
    let mut result = Vec::new();
    for line in stack.lines().take(64) {
        if !line.trim_start().starts_with("at ") {
            continue;
        }
        let Some((_, coordinate)) = line.rsplit_once("eval_script:") else {
            continue;
        };
        let coordinate = coordinate.trim().trim_end_matches(')').trim_end();
        let Some((row, col)) = coordinate.split_once(':') else {
            continue;
        };
        let (Ok(row), Ok(col)) = (row.parse::<u32>(), col.parse::<u32>()) else {
            continue;
        };
        if row == 0 || col == 0 {
            continue;
        }
        let Some(source_line) = source_line(source, row) else {
            continue;
        };
        // QuickJS-ng 0.16.2 records byte-based columns (mark - eol).
        // Convert only valid UTF-8 boundaries to the API's UTF-16 coordinates.
        let Some(prefix) = source_line.get(..col.saturating_sub(1) as usize) else {
            continue;
        };
        result.push(ErrorFrame {
            line: row,
            column: prefix.encode_utf16().count() as u32 + 1,
        });
        if result.len() == MAX_ERROR_FRAMES {
            break;
        }
    }
    result
}

fn source_line(source: &str, wanted: u32) -> Option<&str> {
    let mut row = 1;
    let mut start = 0;
    let mut chars = source.char_indices().peekable();
    while let Some((offset, c)) = chars.next() {
        if matches!(c, '\n' | '\r' | '\u{2028}' | '\u{2029}') {
            if row == wanted {
                return Some(&source[start..offset]);
            }
            start = offset + c.len_utf8();
            if c == '\r' && chars.peek().is_some_and(|(_, c)| *c == '\n') {
                chars.next();
                start += 1;
            }
            row += 1;
        }
    }
    (row == wanted).then_some(&source[start..])
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scripts_diagnostics_strip_names_and_validate_locations() {
        let source = "line one\nline two\n";
        let out = frames("secret\n at SECRET(eval_script:2:4)\n at /private:1:2\n at fake(eval_script:999:1)\n at fake(eval_script:1:999)\n at fake(eval_script:0:1)",source);
        assert_eq!(out.len(), 1);
        assert_eq!((out[0].line, out[0].column), (2, 4));
        assert_eq!(
            frames(&" at private(eval_script:1:1)\n".repeat(100), source).len(),
            MAX_ERROR_FRAMES
        );
    }
    #[test]
    fn scripts_diagnostics_normalize_unicode_columns_and_js_line_terminators() {
        let source = "😀中 abc\r\nx\ry\u{2028}z\u{2029}tail";
        let result = frames(" at f(eval_script:1:8)\n at f(eval_script:1:2)\n at f(eval_script:2:2)\n at f(eval_script:3:1)\n at f(eval_script:4:1)\n at f(eval_script:5:2)", source);
        let positions: Vec<_> = result.iter().map(|f| (f.line, f.column)).collect();
        assert_eq!(positions, [(1, 4), (2, 2), (3, 1), (4, 1), (5, 2)]);
        assert_eq!(source_line("a\r\n", 2), Some(""));
        assert_eq!(source_line("a\r\n", 3), None);
    }
    #[test]
    fn scripts_diagnostics_never_invoke_thrown_object_hooks() {
        for source in [
            "globalThis.touched=0;const e=new Error('PRIVATE');Object.defineProperty(e,'stack',{get(){touched++;throw 1}});Object.defineProperty(e,'message',{get(){touched++;throw 1}});throw e;",
            "globalThis.touched=0;throw new Proxy(new Error('PRIVATE'),{get(){touched++;throw 1},getOwnPropertyDescriptor(){touched++;throw 1}});",
            "globalThis.touched=0;throw {get stack(){touched++;throw 1},toString(){touched++;throw 1}};",
        ] {
            crate::with_runtime(100,|ctx| {
                let getter: Function = ctx.eval("Object.getOwnPropertyDescriptor(Error.prototype,'stack').get").unwrap();
                assert!(ctx.eval::<(),_>(source).is_err());
                let diagnostic = Diagnostics::new(getter,source);
                let failure = diagnostic.capture(&ctx,ErrorPhase::Call,"execution");
                assert_eq!(ctx.globals().get::<_,u32>("touched").unwrap(),0);
                let encoded = serde_json::to_string(&failure).unwrap();
                assert!(!encoded.contains("PRIVATE"));
                if source.contains("defineProperty") { assert!(!failure.frames.is_empty()); }
                else { assert!(failure.frames.is_empty()); }
                Ok(())
            }).unwrap();
        }
    }
}
