// The last expression is the exports object. Int64/UInt64/timestamps use BigInt.
({
    double(value) { return value * 2n; },
    upper(value) { return value.toUpperCase(); }
})
