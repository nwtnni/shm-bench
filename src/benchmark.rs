use core::sync::atomic::Ordering;
use std::env;
use std::thread;
use std::time::Instant;

use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::Allocator as _;
use crate::Observation;
use crate::Output;
use crate::OutputProcess;
use crate::OutputThread;
use crate::Perf;
use crate::ResourceUsage;
use crate::allocator;
use crate::allocator::Backend;
use crate::config;

pub mod memcached;
mod mstress;
mod thread_test;
mod xmalloc;
pub mod ycsb_load;
pub mod ycsb_run;

pub use mstress::Mstress;
pub use thread_test::ThreadTest;
pub use xmalloc::Xmalloc;

pub fn run<B: Benchmark<A>, A: allocator::Backend>(
    benchmark: &B,
    date: u128,
    config: &config::Process,
    allocator: &allocator::Config<A::Config>,
) {
    crate::PROCESS_ID.store(config.process_id, Ordering::Relaxed);
    crate::PROCESS_COUNT.store(config.process_count, Ordering::Relaxed);
    crate::THREAD_COUNT.store(config.thread_count, Ordering::Relaxed);

    let thread_count_per_process = config.thread_count_per_process() as u64 + 1;
    let thread_count = config.process_count as u64 * thread_count_per_process;

    // External coordinator must bootstrap barrier synchronization
    let mut barrier_process = shm::Barrier::builder()
        .name("barrier-process".to_owned())
        .create(false)
        .thread_count(config.process_count as u32)
        .build()
        .unwrap();

    let mut barrier_thread = shm::Barrier::builder()
        .name("barrier-thread".to_owned())
        .create(false)
        .thread_count(thread_count as u32)
        .build()
        .unwrap();

    // Prevent race conditions between creating and opening shared memory data structures
    let (backend, global) = match config.is_leader() {
        true => {
            let backend = A::new(true, allocator, B::NAME).unwrap();
            let global = benchmark.setup_global(config, allocator);
            let _ = barrier_process.wait().unwrap();

            (backend, global)
        }
        false => {
            let _ = barrier_process.wait().unwrap();
            let backend = A::new(false, allocator, B::NAME).unwrap();
            let global = benchmark.setup_global(config, allocator);

            (backend, global)
        }
    };

    let process = benchmark.setup_process(config, allocator);

    let cores = &core_affinity::get_core_ids().unwrap_or_default();

    let mut perf = match (
        config.is_leader(),
        env::var("PERF_CTL_FIFO"),
        env::var("PERF_ACK_FIFO"),
    ) {
        (true, Ok(ctl), Ok(ack)) => Some(Perf::new(ctl, ack)),
        _ => None,
    };

    thread::scope(|scope| {
        let workers = (config.process_id * config.thread_count_per_process()..)
            .take(config.thread_count_per_process())
            .map(|thread_id| {
                let barrier_thread = &barrier_thread;
                let backend = &backend;
                let global = &global;
                let process = &process;
                let handle = scope.spawn(move || {
                    crate::THREAD_ID.set(Some(thread_id));

                    let config = config::Thread {
                        process: *config,
                        thread_id,
                    };
                    let core = thread_id % cores.len();
                    assert!(core_affinity::set_for_current(cores[core]));

                    let mut allocator = backend.allocator(thread_id);
                    let mut worker =
                        benchmark.setup_worker(&config, global, process, &mut allocator);

                    let _ = barrier_thread.wait().unwrap();
                    let before = ResourceUsage::new().unwrap();
                    let start = Instant::now();

                    let output =
                        benchmark.run_worker(&config, global, process, &mut worker, &mut allocator);

                    let time = start.elapsed().as_nanos();
                    let after = ResourceUsage::new().unwrap();
                    let _ = barrier_thread.wait().unwrap();

                    let allocator = allocator.report();

                    (thread_id, after - before, time, output, allocator)
                });
                handle
            })
            .collect::<Vec<_>>();

        let coordinator = scope.spawn(|| {
            let mut coordinator = benchmark.setup_coordinator(config, &global, &process);

            if let Some(perf) = &mut perf {
                perf.enable();
            }

            let _ = barrier_thread.wait().unwrap();
            let output = benchmark.run_coordinator(config, &global, &process, &mut coordinator);
            let _ = barrier_thread.wait().unwrap();

            if let Some(perf) = &mut perf {
                perf.disable();
            }

            output
        });

        let output_workers = workers
            .into_iter()
            .map(|handle| handle.join())
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        let output_coordinator = coordinator.join().unwrap();

        let mut stdout = std::io::stdout().lock();
        serde_json::ser::to_writer(
            &mut stdout,
            &Observation {
                date,
                cargo: crate::Cargo::default(),
                r#global: config.global,
                allocator: serde_json::to_value(allocator).unwrap(),
                benchmark: serde_json::to_value(Named {
                    name: B::NAME,
                    inner: benchmark,
                })
                .unwrap(),
                output: Output {
                    process: OutputProcess {
                        id: config.process_id,
                        output: serde_json::to_value(output_coordinator).unwrap(),
                        allocator: backend.report(),
                    },
                    thread: output_workers
                        .into_iter()
                        .map(
                            |(id, resource_usage, time, output, allocator)| OutputThread {
                                id,
                                resource_usage,
                                time,
                                output: serde_json::to_value(output).unwrap(),
                                allocator,
                            },
                        )
                        .collect(),
                },
            },
        )
        .unwrap();
    });

    benchmark.teardown_process(config, &global, process);

    benchmark.teardown_global(config, global);

    if config.is_leader() {
        barrier_thread.unlink().unwrap();
        barrier_process.unlink().unwrap();
        backend.unlink().unwrap();
    }
}

pub trait Benchmark<B: Backend>: Sync + Serialize {
    const NAME: &str;

    type StateGlobal: Sync;
    type StateProcess: Sync;
    type StateCoordinator;
    type StateWorker;

    type OutputWorker: Send + DeserializeOwned + Serialize;
    type OutputCoordinator: Send + DeserializeOwned + Serialize;

    fn setup_global(
        &self,
        config: &config::Process,
        allocator: &allocator::Config<B::Config>,
    ) -> Self::StateGlobal;

    fn setup_process(
        &self,
        config: &config::Process,
        allocator: &allocator::Config<B::Config>,
    ) -> Self::StateProcess;

    fn setup_coordinator(
        &self,
        config: &config::Process,
        global: &Self::StateGlobal,
        process: &Self::StateProcess,
    ) -> Self::StateCoordinator;

    fn setup_worker(
        &self,
        config: &config::Thread,
        global: &Self::StateGlobal,
        process: &Self::StateProcess,
        allocator: &mut B::Allocator,
    ) -> Self::StateWorker;

    fn run_coordinator(
        &self,
        _config: &config::Process,
        _global: &Self::StateGlobal,
        _process: &Self::StateProcess,
        _coordinator: &mut Self::StateCoordinator,
    ) -> Self::OutputCoordinator;

    fn run_worker(
        &self,
        config: &config::Thread,
        global: &Self::StateGlobal,
        process: &Self::StateProcess,
        worker: &mut Self::StateWorker,
        allocator: &mut B::Allocator,
    ) -> Self::OutputWorker;

    fn teardown_process(
        &self,
        _config: &config::Process,
        _global: &Self::StateGlobal,
        _process: Self::StateProcess,
    ) {
    }

    fn teardown_global(&self, _config: &config::Process, _global: Self::StateGlobal) {}
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
struct Named<T> {
    name: &'static str,
    #[serde(flatten)]
    inner: T,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case", tag = "name")]
pub enum Config {
    Mstress(Mstress),
    Memcached(memcached::Config),
    ThreadTest(thread_test::ThreadTest),
    YcsbRun(ycsb_run::Config),
    YcsbLoad(ycsb_run::Config),
    Xmalloc(xmalloc::Xmalloc),
}
