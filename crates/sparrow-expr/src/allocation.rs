//! Separate a value's resident/backing size from NEW allocations. A shared
//! Column/Literal still carries its size into a later lower/upper/cast.
use crate::{semantics::OutputAllocation, BoundExpr};
use sparrow_model::{DataType, Scalar};

#[derive(Clone, Debug)]
enum Shape {
    Column(usize),
    Literal(usize),
    Scalar,
    Forward,
    Utf8,
    CastUtf8,
}
#[derive(Clone, Debug)]
pub struct AllocationBound {
    shape: Shape,
    children: Vec<Self>,
    call: bool,
}
#[derive(Clone, Copy, Debug, Default)]
pub struct Estimate {
    pub value: usize,
    pub allocated: usize,
}

impl AllocationBound {
    pub fn for_expr(expr: &BoundExpr) -> Self {
        use BoundExpr::*;
        let (shape, children, call) = match expr {
            Column { index } => (Shape::Column(*index), vec![], false),
            Literal(value) => (Shape::Literal(value.resident_bytes()), vec![], false),
            Binary { left, right, .. } => (
                Shape::Scalar,
                vec![Self::for_expr(left), Self::for_expr(right)],
                false,
            ),
            Call { name, args } => {
                let shape = match crate::semantics::function(name).map(|f| f.allocation) {
                    Some(OutputAllocation::Scalar) => Shape::Scalar,
                    Some(OutputAllocation::AsciiUtf8) => Shape::Utf8,
                    _ => Shape::Forward,
                };
                (shape, args.iter().map(Self::for_expr).collect(), true)
            }
            Cast { expr, target } | TryCast { expr, target } => (
                if *target == DataType::Utf8 {
                    Shape::CastUtf8
                } else {
                    Shape::Forward
                },
                vec![Self::for_expr(expr)],
                false,
            ),
            DynamicGet { expr, .. } => (Shape::Forward, vec![Self::for_expr(expr)], false),
            IsNull(expr) | IsNotNull(expr) | Not(expr) => {
                (Shape::Scalar, vec![Self::for_expr(expr)], false)
            }
        };
        Self {
            shape,
            children,
            call,
        }
    }

    pub fn estimate(&self, columns: &[usize]) -> Estimate {
        const SCALAR: usize = std::mem::size_of::<Scalar>();
        match self.shape {
            Shape::Column(index) => {
                return Estimate {
                    value: columns.get(index).copied().unwrap_or(SCALAR),
                    allocated: 0,
                }
            }
            Shape::Literal(bytes) => {
                return Estimate {
                    value: bytes,
                    allocated: 0,
                }
            }
            _ => {}
        }
        let mut value = SCALAR;
        let mut allocated = if self.call {
            self.children
                .len()
                .saturating_mul(SCALAR)
                .saturating_add(64)
        } else {
            0
        };
        for child in &self.children {
            let e = child.estimate(columns);
            value = value.max(e.value);
            allocated = allocated.saturating_add(e.allocated);
        }
        match self.shape {
            Shape::Scalar => value = SCALAR,
            Shape::Utf8 => allocated = allocated.saturating_add(value.saturating_mul(2)),
            Shape::CastUtf8 => {
                value = value.saturating_add(128);
                allocated = allocated.saturating_add(value.saturating_mul(2));
            }
            _ => {}
        }
        Estimate { value, allocated }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn r10_shared_values_keep_size_for_downstream_allocations() {
        let column = BoundExpr::Column { index: 0 };
        let shared = AllocationBound::for_expr(&column).estimate(&[16000]);
        assert_eq!(shared.value, 16000);
        assert_eq!(shared.allocated, 0);
        let lower = BoundExpr::Call {
            name: "lower".into(),
            args: vec![column],
        };
        assert!(
            AllocationBound::for_expr(&lower)
                .estimate(&[shared.value])
                .allocated
                >= 32000
        );
        let literal = BoundExpr::Literal(Scalar::utf8("x".repeat(16000)));
        let shared = AllocationBound::for_expr(&literal).estimate(&[]);
        assert!(shared.value >= 16000);
        assert_eq!(shared.allocated, 0);
        assert!(
            AllocationBound::for_expr(&BoundExpr::Call {
                name: "upper".into(),
                args: vec![literal]
            })
            .estimate(&[])
            .allocated
                >= 32000
        );
    }
}
