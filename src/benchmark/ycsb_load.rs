use core::ops::Deref;

use serde::Deserialize;
use serde::Serialize;

use crate::Index;
use crate::allocator;
use crate::allocator::Backend;
use crate::benchmark;
use crate::config;
use crate::index;

#[derive(Serialize)]
pub struct Config(pub super::ycsb_run::Config);

impl Deref for Config {
    type Target = super::ycsb_run::Config;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct Global<I> {
    index: I,
}

unsafe impl<I> Sync for Global<I> {}

#[derive(Deserialize, Serialize)]
pub struct OutputWorker {
    operation_count: u64,
}

impl<B: Backend, I: Index<B::Allocator>> benchmark::Benchmark<B> for index::Capture<Config, I> {
    const NAME: &str = "ycsb-load";
    type StateGlobal = Global<I>;
    type StateProcess = ();
    type StateCoordinator = ();
    type StateWorker = ();

    type OutputWorker = OutputWorker;
    type OutputCoordinator = ();

    fn setup_global(
        &self,
        config: &config::Process,
        allocator: &allocator::Config<B::Config>,
    ) -> Self::StateGlobal {
        Global {
            index: I::new(
                allocator.numa.clone(),
                self.index.len,
                config.is_leader(),
                self.index.populate,
                config.thread_count,
            )
            .unwrap(),
        }
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
        _config: &config::Thread,
        _global: &Self::StateGlobal,
        (): &Self::StateProcess,
        _allocator: &mut <B as allocator::Backend>::Allocator,
    ) -> Self::StateWorker {
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
        config: &config::Thread,
        global: &Self::StateGlobal,
        (): &Self::StateProcess,
        _worker: &mut Self::StateWorker,
        allocator: &mut <B as allocator::Backend>::Allocator,
    ) -> Self::OutputWorker {
        super::ycsb_run::load(&self.workload, config, allocator, &global.index);
        OutputWorker {
            operation_count: (self.workload.record_count / config.thread_count) as u64,
        }
    }

    fn teardown_global(&self, config: &config::Process, mut global: Self::StateGlobal) {
        if !config.is_leader() {
            return;
        }

        global.index.unlink().unwrap();
    }
}
