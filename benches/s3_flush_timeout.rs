//! Opt-in live S3/R2 fixed sweep.
#[path = "support/runner.rs"]
mod runner;
mod support;

fn main() -> support::Result {
    runner::main(support::Sweep::Flush)
}
