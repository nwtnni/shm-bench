pub mod allocator;
pub mod benchmark;
pub mod config;
mod ebr;
pub mod index;

pub use allocator::Allocator;
pub use index::Index;

use core::cell::Cell;
use core::mem::MaybeUninit;
use core::ops::Sub;
use core::sync::atomic::AtomicUsize;
use std::fs::File;
use std::io;
use std::io::Read as _;
use std::io::Write as _;
use std::path::Path;

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

    memory: MemoryUsage,

    #[serde(flatten)]
    output: serde_json::Value,

    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    allocator: serde_json::Value,
}

#[derive(Deserialize, Serialize)]
pub struct OutputThread {
    id: usize,

    resource: ResourceUsage,

    time: u128,

    #[serde(flatten)]
    output: serde_json::Value,

    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    allocator: serde_json::Value,
}

pub(crate) struct Perf {
    ctl: File,
    ack: File,
}

impl Perf {
    pub fn new<C: AsRef<Path>, A: AsRef<Path>>(ctl: C, ack: A) -> Self {
        let ctl = ctl.as_ref();
        let ack = ack.as_ref();

        Self {
            ctl: File::options()
                .write(true)
                .open(ctl)
                .unwrap_or_else(|error| {
                    panic!(
                        "Failed to open perf control file {}: {}",
                        ctl.display(),
                        error,
                    )
                }),
            ack: File::options()
                .read(true)
                .open(ack)
                .unwrap_or_else(|error| {
                    panic!("Failed to open perf ack file {}: {}", ack.display(), error,)
                }),
        }
    }

    pub fn enable(&mut self) {
        self.ctl
            .write_all(b"enable\n\0")
            .expect("Failed to write to perf ctl file");
        self.wait();
    }

    pub fn disable(&mut self) {
        self.ctl
            .write_all(b"disable\n\0")
            .expect("Failed to write to perf ctl file");
        self.wait();
    }

    fn wait(&mut self) {
        match self.ack.read_exact(&mut [0u8; 5]) {
            Ok(()) => (),
            Err(error) => panic!("Failed to read from perf ack file: {}", error),
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub struct ResourceUsage {
    max_rss: u64,
    user_time: u128,
    system_time: u128,
    minor_fault_count: u64,
    major_fault_count: u64,
    voluntary_context_switch_count: u64,
    involuntary_context_switch_count: u64,
}

impl Sub for ResourceUsage {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            max_rss: self.max_rss.max(rhs.max_rss),
            user_time: self.user_time.saturating_sub(rhs.user_time),
            system_time: self.system_time.saturating_sub(rhs.system_time),
            minor_fault_count: self.minor_fault_count.saturating_sub(rhs.minor_fault_count),
            major_fault_count: self.major_fault_count.saturating_sub(rhs.major_fault_count),
            voluntary_context_switch_count: self
                .voluntary_context_switch_count
                .saturating_sub(rhs.voluntary_context_switch_count),
            involuntary_context_switch_count: self
                .involuntary_context_switch_count
                .saturating_sub(rhs.involuntary_context_switch_count),
        }
    }
}

impl ResourceUsage {
    pub(crate) fn new() -> io::Result<Self> {
        let rusage = unsafe {
            let mut rusage = MaybeUninit::<libc::rusage>::zeroed();
            match libc::getrusage(libc::RUSAGE_THREAD, rusage.as_mut_ptr()) {
                0 => rusage.assume_init(),
                _ => return Err(io::Error::last_os_error()),
            }
        };

        Ok(Self {
            max_rss: rusage.ru_maxrss as u64 * 2u64.pow(10),
            user_time: Self::timeval_as_nanos(rusage.ru_utime),
            system_time: Self::timeval_as_nanos(rusage.ru_stime),
            minor_fault_count: rusage.ru_minflt as u64,
            major_fault_count: rusage.ru_majflt as u64,
            voluntary_context_switch_count: rusage.ru_nvcsw as u64,
            involuntary_context_switch_count: rusage.ru_nivcsw as u64,
        })
    }

    fn timeval_as_nanos(time: libc::timeval) -> u128 {
        let s = time.tv_sec as u128 * 10u128.pow(9);
        let us = time.tv_usec as u128 * 10u128.pow(6);
        s + us
    }
}

#[derive(Clone, Default, Deserialize, Serialize)]
pub struct MemoryUsage {
    pss: u64,
    pss_dirty: u64,
    shared_clean: u64,
    shared_dirty: u64,
    private_clean: u64,
    private_dirty: u64,
}

impl MemoryUsage {
    pub fn new<F: Fn(&Mapping) -> bool>(filter: F) -> anyhow::Result<Self> {
        let mut total = MemoryUsage::default();
        smaps::read_filter(Path::new("/proc/self/smaps"), filter)?
            .into_iter()
            .for_each(|(_, usage)| {
                total.pss += usage.pss as u64;
                total.pss_dirty += usage.pss_dirty as u64;
                total.shared_clean += usage.shared_clean as u64;
                total.shared_dirty += usage.shared_dirty as u64;
                total.private_clean += usage.private_clean as u64;
                total.private_dirty += usage.private_dirty as u64;
            });

        Ok(total)
    }
}
