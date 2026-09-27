//! Descriptors for existing pure functions, not a new VM or backend contract.
//! Changes to evaluation results/order also require a state-semantics revision.
pub const VERSION: u32 = 1;
pub const EVALUATION: &str = "eager_left_to_right_first_error";
#[derive(Clone, Copy, Debug)]
pub enum OutputAllocation {
    Scalar,
    AsciiUtf8,
    Forward,
    BoundedText,
    Replace,
    JsonParse,
    JsonObject,
    JsonStringify,
    FixedText,
    Collection,
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
    bounded(
        "array_length",
        1,
        1,
        "array_or_dynamic_array",
        "propagate",
        OutputAllocation::Scalar,
    ),
    bounded(
        "array_get",
        2,
        2,
        "array_zero_based_int64_index",
        "missing_or_null_is_sql_null",
        OutputAllocation::Forward,
    ),
    bounded(
        "array_contains",
        2,
        2,
        "array_typed_structural_equality",
        "propagate",
        OutputAllocation::Scalar,
    ),
    bounded(
        "array_append",
        2,
        2,
        "array_value",
        "null_array_propagates_null_element_preserved",
        OutputAllocation::Collection,
    ),
    bounded(
        "array_slice",
        3,
        3,
        "array_nonnegative_start_count",
        "propagate",
        OutputAllocation::Collection,
    ),
    bounded(
        "array_concat",
        2,
        2,
        "two_arrays",
        "propagate",
        OutputAllocation::Collection,
    ),
    bounded(
        "array_join",
        2,
        2,
        "array_of_utf8_and_utf8_separator",
        "propagate_null_element",
        OutputAllocation::Collection,
    ),
    bounded(
        "object_keys",
        1,
        1,
        "object_insertion_order",
        "propagate",
        OutputAllocation::Collection,
    ),
    bounded(
        "object_values",
        1,
        1,
        "object_insertion_order",
        "propagate",
        OutputAllocation::Collection,
    ),
    bounded(
        "object_get",
        2,
        2,
        "object_utf8_key",
        "missing_or_null_is_sql_null",
        OutputAllocation::Forward,
    ),
    bounded(
        "object_has_key",
        2,
        2,
        "object_utf8_key",
        "propagate",
        OutputAllocation::Scalar,
    ),
    bounded(
        "object_remove",
        2,
        2,
        "object_utf8_key",
        "propagate",
        OutputAllocation::Collection,
    ),
    bounded(
        "object_set",
        3,
        3,
        "object_utf8_key_value_unique_keys",
        "null_object_or_key_propagates_null_value_preserved",
        OutputAllocation::Collection,
    ),
    bounded(
        "split",
        2,
        2,
        "utf8_nonempty_literal_separator_max_1024_parts",
        "propagate",
        OutputAllocation::Collection,
    ),
    bounded(
        "base64_encode",
        1,
        1,
        "utf8_or_bytes_rfc4648_standard_padded",
        "propagate",
        OutputAllocation::Collection,
    ),
    bounded(
        "base64_decode",
        1,
        1,
        "strict_rfc4648_standard_padded_utf8",
        "propagate",
        OutputAllocation::Collection,
    ),
    bounded(
        "hex_encode",
        1,
        1,
        "utf8_or_bytes_lowercase",
        "propagate",
        OutputAllocation::Collection,
    ),
    bounded(
        "hex_decode",
        1,
        1,
        "even_ascii_hex_case_insensitive",
        "propagate",
        OutputAllocation::Collection,
    ),
    bounded(
        "sha256",
        1,
        1,
        "utf8_or_bytes_sha256_lowercase_hex",
        "propagate",
        OutputAllocation::FixedText,
    ),
    bounded(
        "concat",
        2,
        16,
        "utf8",
        "propagate",
        OutputAllocation::BoundedText,
    ),
    bounded(
        "substring",
        3,
        3,
        "utf8_positive_1_based_start_nonnegative_count_unicode_scalars",
        "propagate",
        OutputAllocation::BoundedText,
    ),
    bounded(
        "replace",
        3,
        3,
        "utf8_nonempty_literal_search",
        "propagate",
        OutputAllocation::Replace,
    ),
    bounded(
        "contains",
        2,
        2,
        "utf8_literal",
        "propagate",
        OutputAllocation::Scalar,
    ),
    bounded(
        "starts_with",
        2,
        2,
        "utf8_literal",
        "propagate",
        OutputAllocation::Scalar,
    ),
    bounded(
        "ends_with",
        2,
        2,
        "utf8_literal",
        "propagate",
        OutputAllocation::Scalar,
    ),
    bounded(
        "trim",
        1,
        1,
        "utf8_unicode_whitespace",
        "propagate",
        OutputAllocation::BoundedText,
    ),
    bounded(
        "round",
        1,
        1,
        "finite_numeric_ties_away_from_zero",
        "propagate",
        OutputAllocation::Scalar,
    ),
    bounded(
        "floor",
        1,
        1,
        "finite_numeric",
        "propagate",
        OutputAllocation::Scalar,
    ),
    bounded(
        "ceil",
        1,
        1,
        "finite_numeric",
        "propagate",
        OutputAllocation::Scalar,
    ),
    bounded(
        "to_int64",
        1,
        1,
        "numeric_or_text_strict_range_truncate_toward_zero",
        "propagate_including_dynamic_null",
        OutputAllocation::Scalar,
    ),
    bounded(
        "to_float64",
        1,
        1,
        "numeric_or_text_finite_lossy_integer_conversion",
        "propagate_including_dynamic_null",
        OutputAllocation::Scalar,
    ),
    bounded(
        "to_string",
        1,
        1,
        "finite_primitive",
        "propagate_including_dynamic_null",
        OutputAllocation::BoundedText,
    ),
    bounded(
        "json_get",
        2,
        2,
        "strict_json_utf8_rfc6901_pointer",
        "missing_or_json_null_is_sql_null",
        OutputAllocation::JsonParse,
    ),
    bounded(
        "json_object",
        2,
        16,
        "alternating_utf8_keys_values_unique_keys",
        "null_key_propagates_null_value_preserved",
        OutputAllocation::JsonObject,
    ),
    bounded(
        "json_stringify",
        1,
        1,
        "scalar_json_bytes_base64_nonfinite_rejected",
        "sql_null_becomes_json_null_text",
        OutputAllocation::JsonStringify,
    ),
    bounded(
        "parse_timestamp",
        1,
        1,
        "rfc3339_known_offset_years_1_9999_no_leap_no_submicrosecond",
        "propagate",
        OutputAllocation::Scalar,
    ),
    bounded(
        "format_timestamp",
        1,
        1,
        "utc_timestamp_or_int64_micros_years_1_9999",
        "propagate",
        OutputAllocation::FixedText,
    ),
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
const fn bounded(
    name: &'static str,
    min_args: usize,
    max_args: usize,
    input: &'static str,
    null_policy: &'static str,
    allocation: OutputAllocation,
) -> FunctionSemantics {
    FunctionSemantics {
        name,
        min_args,
        max_args: Some(max_args),
        input,
        null_policy,
        allocation,
        output_bound:
            "scalar_or_max_65536_bytes_text_or_dynamic_resident; reservation_may_be_stricter",
        work_bound:
            "max_16_arguments_65536_bytes_each; json_depth_8; pointer_1024_bytes; no_regex_or_io",
    }
}
pub fn function(name: &str) -> Option<&'static FunctionSemantics> {
    FUNCTIONS.iter().find(|f| f.name.eq_ignore_ascii_case(name))
}

#[cfg(test)]
mod tests {
    #[test]
    fn r10_every_registered_function_has_an_evaluator_and_allocation_rule() {
        for f in super::FUNCTIONS {
            if matches!(f.allocation, super::OutputAllocation::AsciiUtf8) {
                assert_eq!(
                    (f.min_args, f.max_args),
                    (1, Some(1)),
                    "ASCII allocation bound only covers unary functions"
                );
            }
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
