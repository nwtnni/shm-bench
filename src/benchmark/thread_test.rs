// https://github.com/emeryberger/Hoard/blob/f021bdb810332c9c9f5a11ae5404aaa38fe129c0/benchmarks/threadtest/threadtest.cpp

use core::cmp;

use bon::Builder;
use serde::Deserialize;
use serde::Serialize;

use crate::Allocator;
use crate::allocator;
use crate::allocator::Backend;
use crate::allocator::Handle as _;
use crate::benchmark;
use crate::config;
use crate::measure;

#[derive(Builder, Clone, Debug, Deserialize, Serialize)]
pub struct ThreadTest {
    #[builder(default = 100)]
    pub(crate) iteration_count: u64,

    #[builder(default = 100_000)]
    pub(crate) operation_count: u64,

    #[builder(default = 8)]
    pub object_size: usize,
}

#[derive(Deserialize, Serialize)]
pub struct OutputWorker {
    operation_count: u64,
    size: u64,
}

pub struct Worker<A: Allocator> {
    iteration_count: usize,
    handles: Vec<Option<A::Handle>>,
}

impl<B: Backend> benchmark::Benchmark<B> for ThreadTest {
    const NAME: &str = "thread-test";
    type StateGlobal = ();
    type StateProcess = ();
    type StateCoordinator = ();
    type StateWorker = Worker<B::Allocator>;

    type OutputWorker = OutputWorker;
    type OutputCoordinator = ();

    fn setup_global(
        &self,
        _config: &config::Process,
        _allocator: &allocator::Config<B::Config>,
    ) -> Self::StateGlobal {
    }

    fn setup_process(
        &self,
        _config: &config::Process,
        _allocator: &allocator::Config<B::Config>,
    ) -> Self::StateProcess {
    }

    fn setup_coordinator(
        &self,
        _config: &config::Process,
        _global: &Self::StateGlobal,
        (): &Self::StateProcess,
    ) -> Self::StateCoordinator {
    }

    fn setup_worker(
        &self,
        config: &config::Thread,
        (): &Self::StateGlobal,
        (): &Self::StateProcess,
        _allocator: &mut measure::time::Allocator<<B as allocator::Backend>::Allocator>,
    ) -> Self::StateWorker {
        Worker {
            iteration_count: self.iteration_count as usize / config.thread_count,
            handles: (0..self.operation_count).map(|_| None).collect(),
        }
    }

    fn run_coordinator(
        &self,
        _config: &config::Process,
        _global: &Self::StateGlobal,
        (): &Self::StateProcess,
        _coordinator: &mut Self::StateCoordinator,
    ) -> Self::OutputCoordinator {
    }

    fn run_worker(
        &self,
        _config: &config::Thread,
        _: &Self::StateGlobal,
        (): &Self::StateProcess,
        worker: &mut Self::StateWorker,
        allocator: &mut measure::time::Allocator<<B as allocator::Backend>::Allocator>,
    ) -> Self::OutputWorker {
        for _ in 0..worker.iteration_count {
            for handle in &mut worker.handles {
                *handle = allocator.allocate(self.object_size);

                unsafe {
                    libc::memset(
                        handle.as_mut().unwrap().as_ptr(),
                        0xff,
                        cmp::min(self.object_size, 64),
                    );
                }
            }

            for handle in &mut worker.handles {
                let handle = handle.take().unwrap();
                unsafe {
                    allocator.deallocate(handle);
                }
            }
        }

        let operation_count = (worker.handles.len() * worker.iteration_count) as u64;
        OutputWorker {
            operation_count,
            size: operation_count * self.object_size as u64,
        }
    }
}
