//! Descriptors for existing pure functions, not a new VM or backend contract.
//! Changes to evaluation results/order also require a state-semantics revision.
pub const VERSION: u32 = 1;
pub const EVALUATION: &str = "eager_left_to_right_first_error";
#[derive(Clone, Copy, Debug)]
pub enum OutputAllocation {
    Scalar,
    AsciiUtf8,
    Forward,
}
#[derive(Clone, Copy, Debug)]
pub struct FunctionSemantics {
    pub name: &'static str,
    pub min_args: usize,
    pub max_args: Option<usize>,
    pub input: &'static str,
    pub null_policy: &'static str,
    pub output_bound: &'static str,
    pub work_bound: &'static str,
    pub allocation: OutputAllocation,
}
pub const FUNCTIONS: &[FunctionSemantics] = &[
    FunctionSemantics {
        name: "abs",
        min_args: 1,
        max_args: Some(1),
        input: "int64_or_float64",
        null_policy: "propagate; int64_min_overflows",
        output_bound: "one_numeric_value",
        allocation: OutputAllocation::Scalar,
        work_bound: "constant",
    },
    FunctionSemantics {
        name: "lower",
        min_args: 1,
        max_args: Some(1),
        input: "utf8_ascii_case_mapping",
        null_policy: "propagate",
        output_bound: "input_utf8_bytes",
        allocation: OutputAllocation::AsciiUtf8,
        work_bound: "linear_input_bytes",
    },
    FunctionSemantics {
        name: "upper",
        min_args: 1,
        max_args: Some(1),
        input: "utf8_ascii_case_mapping",
        null_policy: "propagate",
        output_bound: "input_utf8_bytes",
        allocation: OutputAllocation::AsciiUtf8,
        work_bound: "linear_input_bytes",
    },
    FunctionSemantics {
        name: "length",
        min_args: 1,
        max_args: Some(1),
        input: "utf8_unicode_scalar_count",
        null_policy: "propagate",
        output_bound: "one_int64",
        allocation: OutputAllocation::Scalar,
        work_bound: "linear_input_bytes",
    },
    FunctionSemantics {
        name: "char_length",
        min_args: 1,
        max_args: Some(1),
        input: "utf8_unicode_scalar_count",
        null_policy: "propagate",
        output_bound: "one_int64",
        allocation: OutputAllocation::Scalar,
        work_bound: "linear_input_bytes",
    },
    FunctionSemantics {
        name: "coalesce",
        min_args: 1,
        max_args: None,
        input: "binder_validated_arguments",
        null_policy: "first_non_null; all_arguments_evaluated",
        output_bound: "one_input_value",
        allocation: OutputAllocation::Forward,
        work_bound: "linear_argument_count_plus_child_evaluation",
    },
    FunctionSemantics {
        name: "nullif",
        min_args: 2,
        max_args: Some(2),
        input: "binder_validated_comparable",
        null_policy: "null_on_equal_else_first",
        output_bound: "one_input_value",
        allocation: OutputAllocation::Forward,
        work_bound: "comparison_plus_child_evaluation",
    },
    FunctionSemantics {
        name: "greatest",
        min_args: 1,
        max_args: None,
        input: "comparable",
        null_policy: "skip_null_and_nan; null_if_no_candidate",
        output_bound: "one_input_value",
        allocation: OutputAllocation::Forward,
        work_bound: "linear_comparisons_plus_child_evaluation",
    },
    FunctionSemantics {
        name: "least",
        min_args: 1,
        max_args: None,
        input: "comparable",
        null_policy: "skip_null_and_nan; null_if_no_candidate",
        output_bound: "one_input_value",
        allocation: OutputAllocation::Forward,
        work_bound: "linear_comparisons_plus_child_evaluation",
    },
];
pub fn function(name: &str) -> Option<&'static FunctionSemantics> {
    FUNCTIONS.iter().find(|f| f.name.eq_ignore_ascii_case(name))
}

#[cfg(test)]
mod tests {
    #[test]
    fn r10_every_registered_function_has_an_evaluator_and_allocation_rule() {
        for f in super::FUNCTIONS {
            let values = vec![sparrow_model::Scalar::Null; f.min_args];
            crate::check_call_arity(f.name, values.len()).unwrap();
            assert!(
                crate::eval_call_values(f.name, values).is_ok(),
                "missing evaluator: {}",
                f.name
            );
            let expr = crate::BoundExpr::Call {
                name: f.name.into(),
                args: vec![crate::BoundExpr::Literal(sparrow_model::Scalar::Null); f.min_args],
            };
            let e = crate::allocation::AllocationBound::for_expr(&expr).estimate(&[]);
            assert!(e.value >= std::mem::size_of::<sparrow_model::Scalar>());
        }
        assert!(!crate::check_call_arity("lower", 2)
            .unwrap_err()
            .message
            .contains("Some("));
    }
}
