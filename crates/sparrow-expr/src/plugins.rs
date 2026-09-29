//! Binding-only registry scope. Runtime expressions contain immutable owning
//! handles, never a mutable name lookup. Script cancellation uses a separate
//! task-local scope, not this binding-only thread-local registry.
use crate::Expr;
use sparrow_model::{ErrorCode, Result, Scalar, SparrowError};
pub use sparrow_plugin::*;
use std::{cell::RefCell, sync::Arc};
thread_local! {static BINDING:RefCell<Option<Arc<Manager>>>=const{RefCell::new(None)};}
struct Scope(Option<Arc<Manager>>);
impl Drop for Scope {
    fn drop(&mut self) {
        BINDING.with(|v| *v.borrow_mut() = self.0.take());
    }
}
pub fn with_registry<T>(registry: Option<Arc<Manager>>, f: impl FnOnce() -> T) -> T {
    let _scope = Scope(BINDING.with(|v| v.replace(registry)));
    f()
}
pub fn call(name: String, mut args: Vec<Expr>) -> Result<Expr> {
    if !name.eq_ignore_ascii_case("plugin_call") {
        return Ok(Expr::Call { name, args });
    }
    let error = || {
        SparrowError::new(ErrorCode::InvalidArgument,"plugin_call requires literal package, version, manifest SHA256, function, then 0..8 scalar arguments")
    };
    if !(4..=12).contains(&args.len()) {
        return Err(error());
    }
    let strings = args[..4]
        .iter()
        .map(|e| match e {
            Expr::Literal(Scalar::Utf8(v)) => Ok(v.as_ref()),
            _ => Err(error()),
        })
        .collect::<Result<Vec<_>>>()?;
    let function = BINDING.with(|v| {
        let manager = v.borrow().clone().ok_or_else(|| {
            SparrowError::new(
                ErrorCode::FeatureUnavailable,
                "plugin registry is not configured for this binder",
            )
        })?;
        manager.resolve(strings[0], strings[1], strings[2], strings[3])
    })?;
    args.drain(..4);
    Ok(Expr::Plugin { function, args })
}
