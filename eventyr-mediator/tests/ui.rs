//! The composition's diagnostics, pinned with trybuild: a broken
//! composition must fail with the intended message, at the offending
//! clause, not with a wall of follow-on errors.

#[test]
fn ui() {
    trybuild::TestCases::new().compile_fail("tests/ui/*.rs");
}
