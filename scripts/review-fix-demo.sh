#!/usr/bin/env bash
# Process / API evidence for the static-review fix set (R01–R28 / V01–V03).
# Covers the failure/edge matrix, not only happy-path units.
# cargo test accepts only ONE TESTNAME filter per invocation.
set -euo pipefail
cd "$(dirname "$0")/.."

fail() { echo "FAIL: $*" >&2; exit 1; }

run_filter() {
  local filter="$1"
  echo "== filter ${filter} =="
  cargo test --workspace "${filter}" -- --test-threads=8
}

echo "== named rxx/vxx regressions (one cargo filter each) =="
for f in r01_ r02_ r03_ r04_ r05_ r06_ r07_ r08_ r09_ r10_ \
         r11_ r12_ r13_ r14_ r15_ r16_ r17_ r18_ r19_ r20_ \
         r21_ r22_ r23_ r24_ r25_ r26_ r27_ r28_ v01_ v02_ v03_ \
         p0_1_ p0_2_ p0_3_ p0_4_ p0_5_ p0_6_ p0_7_ p0_8_ p0_9_ a1_ p1_24_ p1_29_ \
         p0_11_ p0_12_ p0_13_ p2_40_ p3_42_ p3_43_ p3_54_ \
         n4_ n7_ n5_ n6_ n8_ n9_ n10_ \
         n11_ n12_ n13_ n14_ n15_ n16_ \
         p1_14_ p1_20_ p1_17_ p1_27_ a2_; do
  run_filter "$f"
done

echo "== HTTP API recovery + metrics (review_api integration) =="
cargo test -p sparrow-server --test review_api -- --nocapture

echo "== review-fix-demo: ok =="
