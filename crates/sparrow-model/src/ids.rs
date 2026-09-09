//! Stable identifier newtypes shared by SQL and Graph front-ends.
//!
//! These are placeholders for M0: they exist so plans, jobs, and schemas can
//! be referenced without stringly-typed IDs. Allocation policy (catalog vs
//! ephemeral) is deferred to M1.

use std::fmt;

macro_rules! id_newtype {
    ($(#[$meta:meta])* $name:ident, $inner:ty) => {
        $(#[$meta])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
        pub struct $name(pub $inner);

        impl $name {
            pub const fn new(raw: $inner) -> Self {
                Self(raw)
            }

            pub const fn raw(self) -> $inner {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), self.0)
            }
        }

        impl From<$inner> for $name {
            fn from(raw: $inner) -> Self {
                Self(raw)
            }
        }
    };
}

id_newtype!(
    /// Identifies a named pipeline (SQL text or Graph definition).
    PipelineId,
    u64
);
id_newtype!(
    /// Monotonic revision of a pipeline definition.
    RevisionId,
    u64
);
id_newtype!(
    /// One in-process execution attempt of a pipeline revision.
    JobAttemptId,
    u64
);
id_newtype!(
    /// Operator node inside a plan. Placeholder until the planner assigns
    /// physical ids in M1.
    OperatorId,
    u32
);
id_newtype!(
    /// Schema identity (catalog or ephemeral).
    SchemaId,
    u32
);
id_newtype!(
    /// Field identity within a schema. Stable across projection when preserved.
    FieldId,
    u16
);
id_newtype!(
    /// Stateful slot inside an operator. Stable across attempts so V1
    /// recovery can address the same map.
    StateSlotId,
    u16
);

/// Compatibility address for a stateful slot: operator + slot.
/// Restore rejects a checkpoint whose slot key does not match the live plan.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct StateSlotKey {
    pub operator: OperatorId,
    pub slot: StateSlotId,
}

impl StateSlotKey {
    pub const fn new(operator: OperatorId, slot: StateSlotId) -> Self {
        Self { operator, slot }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_copy_and_ordered() {
        let a = PipelineId::new(1);
        let b = PipelineId::new(2);
        assert!(a < b);
        assert_eq!(a.raw(), 1);
        assert_eq!(format!("{a}"), "PipelineId(1)");
    }
}
