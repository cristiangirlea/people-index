//! pi-stages: the eight stages of a run (docs/ARCHITECTURE.md, "Stages"), each a
//! [`pi_coord::Stage`], and [`build_registry`] to hand them to an executor.
//!
//! Every task follows the same shape: the async side streams committed inputs from the store
//! and uploads outputs, a blocking thread does the CPU work (gzip, JSON, zstd, the join), and
//! bounded channels between them keep memory at a few segments plus the configured buffers.
//! Outputs are named after the attempt and listed in the task's report; the executor commits
//! them, and later stages read only what commits list.
//!
//! | stage | type |
//! |---|---|
//! | `plan` | [`PlanStage`] |
//! | `extract-orgs` | [`ExtractOrgsStage`] |
//! | `extract-persons` | [`ExtractPersonsStage`] |
//! | `join` | [`JoinStage`] |
//! | `assemble` | [`AssembleStage`] |
//! | `prepare-index` | [`PrepareIndexStage`] |
//! | `load` | [`LoadStage`] |
//! | `publish` | [`PublishStage`] |

mod assemble;
mod channel;
mod channel_reader;
mod commits;
mod config;
pub mod counters;
mod doc_channel;
mod es_error;
mod extract;
mod extract_orgs;
mod extract_persons;
mod feeder;
mod join;
mod lines;
mod load;
mod metered;
mod passes;
mod pipeline;
mod plan;
mod plan_file;
mod prepare_index;
mod publish;
mod quarantine;
mod registry;
mod segments_in;
mod segments_out;
mod summary;
mod uploader;

pub use assemble::AssembleStage;
pub use commits::CommitCache;
pub use config::{
    DEFAULT_BUFFER_BYTES, DEFAULT_LOAD_BUFFER_BULKS, DEFAULT_MAX_LINE_BYTES, DEFAULT_MAX_MALFORMED,
    DEFAULT_MIN_PARTITIONS, DEFAULT_PARTITION_TARGET_BYTES, DEFAULT_TASK_MEMORY_BYTES,
    DEFAULT_UPLOAD_CONCURRENCY, EsStageConfig, StagesConfig,
};
pub use extract_orgs::ExtractOrgsStage;
pub use extract_persons::ExtractPersonsStage;
pub use join::JoinStage;
pub use load::LoadStage;
pub use metered::Metered;
pub use plan::PlanStage;
pub use plan_file::{Feed, InputFile, Plan, index_name};
pub use prepare_index::PrepareIndexStage;
pub use publish::{PublishStage, delete_orphans};
pub use registry::build_registry;
pub use summary::{PublishSummary, StageSummary, Summary};
