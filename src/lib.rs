pub mod allocator;
pub mod benchmark;
pub mod config;
mod ebr;
pub mod index;
pub mod measure;

pub use allocator::Allocator;
pub use index::Index;

use core::cell::Cell;
use core::sync::atomic::AtomicUsize;

use serde::Deserialize;
use serde::Serialize;

pub use smaps::Mapping;

pub static PROCESS_ID: AtomicUsize = AtomicUsize::new(0);
pub static PROCESS_COUNT: AtomicUsize = AtomicUsize::new(0);
pub static THREAD_COUNT: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    pub static THREAD_ID: Cell<Option<usize>> = const { Cell::new(None) };
}

#[derive(Deserialize, Serialize)]
pub struct Observation {
    date: u128,
    cargo: Cargo,
    r#global: config::Global,
    allocator: serde_json::Value,
    benchmark: serde_json::Value,
    output: Output,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Cargo {
    release: bool,
}

impl Default for Cargo {
    fn default() -> Self {
        Self {
            release: !cfg!(debug_assertions),
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct Output {
    process: OutputProcess,
    thread: Vec<OutputThread>,
}

#[derive(Deserialize, Serialize)]
pub struct OutputProcess {
    id: usize,

    memory: measure::memory::Total,

    #[serde(flatten)]
    output: serde_json::Value,

    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    allocator: serde_json::Value,
}

#[derive(Deserialize, Serialize)]
pub struct OutputThread {
    id: usize,

    time: measure::time::Report,

    resource: measure::Resource,

    #[serde(skip_serializing_if = "Option::is_none")]
    perf: Option<measure::perf::Report>,

    #[serde(flatten)]
    output: serde_json::Value,

    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    allocator: serde_json::Value,
}
