//! Macro diagnostics, verified with trybuild: the error cases must
//! fail with the intended message, not a wall of follow-on errors.

#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.pass("tests/pass_conventions.rs");
    t.compile_fail("tests/ui/*.rs");
}
