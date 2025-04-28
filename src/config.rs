use core::ops::Deref;

use serde::Deserialize;
use serde::Serialize;

#[derive(Copy, Clone, Debug, Deserialize, Serialize)]
pub struct Global {
    /// Number of processes
    pub process_count: usize,

    /// Number of threads
    pub thread_count: usize,
}

impl Global {
    pub fn new(process_count: usize, thread_count: usize) -> Self {
        assert_eq!(
            thread_count % process_count,
            0,
            "thread count {} must be evenly divisible by process count {}",
            thread_count,
            process_count
        );

        Self {
            process_count,
            thread_count,
        }
    }

    pub fn thread_count_per_process(&self) -> usize {
        self.thread_count / self.process_count
    }

    pub fn with_process_id(&self, process_id: usize) -> Process {
        Process {
            global: *self,
            process_id,
        }
    }
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize)]
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

#[derive(Copy, Clone, Debug)]
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
