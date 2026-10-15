//! One module per subcommand. Each returns a value; [`crate::run_cli`] turns it into output
//! and an exit code.

pub mod datagen;
pub mod run;
pub mod stage;
pub mod verify;
pub mod worker;
