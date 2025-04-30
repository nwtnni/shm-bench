use core::ops::Deref;

use bon::bon;
use serde::Deserialize;
use serde::Serialize;
use shm::Numa;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Global {
    /// NUMA policy
    pub numa: Option<Numa>,

    /// Number of processes
    pub process_count: usize,

    /// Number of threads
    pub thread_count: usize,
}

#[bon]
impl Global {
    #[builder]
    pub fn new(process_count: usize, thread_count: usize, numa: Option<Numa>) -> Option<Self> {
        match thread_count % process_count {
            0 => Some(Self {
                process_count,
                thread_count,
                numa,
            }),
            _ => None,
        }
    }
}

impl Global {
    pub fn thread_count_per_process(&self) -> usize {
        self.thread_count / self.process_count
    }

    pub fn with_process_id(&self, process_id: usize) -> Process {
        Process {
            global: self.clone(),
            process_id,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Process {
    #[serde(flatten)]
    pub global: Global,

    /// Unique process ID within range 0..process_count
    pub process_id: usize,
}

impl Process {
    pub fn is_leader(&self) -> bool {
        self.process_id == 0
    }
}

impl Deref for Process {
    type Target = Global;
    fn deref(&self) -> &Self::Target {
        &self.global
    }
}

#[derive(Clone, Debug)]
pub struct Thread {
    pub process: Process,

    /// Unique thread ID within range 0..process_count * thread_count
    pub thread_id: usize,
}

impl Deref for Thread {
    type Target = Process;
    fn deref(&self) -> &Self::Target {
        &self.process
    }
}
