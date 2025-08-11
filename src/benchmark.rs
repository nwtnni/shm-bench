use core::mem;
use core::sync::atomic::Ordering;
use std::env;
use std::io;
use std::thread;
use std::time::Instant;

use anyhow::Context as _;
use anyhow::anyhow;
use hwloc2::Topology;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::Allocator as _;
use crate::Observation;
use crate::Output;
use crate::OutputProcess;
use crate::OutputThread;
use crate::allocator;
use crate::allocator::Backend;
use crate::config;
use crate::measure;

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
) -> anyhow::Result<()> {
    crate::PROCESS_ID.store(config.process_id, Ordering::Relaxed);
    crate::PROCESS_COUNT.store(config.process_count, Ordering::Relaxed);
    crate::THREAD_COUNT.store(config.thread_count, Ordering::Relaxed);

    let topology = Topology::new().ok_or_else(|| anyhow!("Failed to retrieve hwloc2 topology"))?;

    let depth = topology
        .depth_for_type(&hwloc2::ObjectType::PU)
        .map_err(|error| anyhow!("Failed to get processing unit depth: {:?}", error))?;
    let cores = topology
        .objects_at_depth(depth)
        .into_iter()
        .map(|core| core.os_index())
        .collect::<Vec<_>>();

    let thread_count_per_process = config.thread_count_per_process() as u64 + 1;
    let thread_count = config.process_count as u64 * thread_count_per_process;

    // External coordinator must bootstrap barrier synchronization
    let mut barrier_process = shm::Barrier::builder()
        .name("barrier-process".to_owned())
        .create(false)
        .thread_count(config.process_count as u32)
        .build()?;

    let mut barrier_thread = shm::Barrier::builder()
        .name("barrier-thread".to_owned())
        .create(false)
        .thread_count(thread_count as u32)
        .build()?;

    // Prevent race conditions between creating and opening shared memory data structures
    let (backend, global) = match config.is_leader() {
        true => {
            let backend = A::new(true, allocator, B::NAME)?;
            let global = benchmark.setup_global(config, allocator);
            let _ = barrier_process.wait()?;

            (backend, global)
        }
        false => {
            let _ = barrier_process.wait()?;
            let backend = A::new(false, allocator, B::NAME)?;
            let global = benchmark.setup_global(config, allocator);

            (backend, global)
        }
    };

    let process = benchmark.setup_process(config, allocator);

    let mut perf_external = match (
        config.is_leader(),
        env::var("PERF_CTL_FIFO"),
        env::var("PERF_ACK_FIFO"),
    ) {
        (true, Ok(ctl), Ok(ack)) => Some(measure::perf::Sync::new(ctl, ack)?),
        _ => None,
    };
    let perf_internal = perf_external.is_none();

    thread::scope(|scope| -> anyhow::Result<_> {
        let workers = (config.process_id * config.thread_count_per_process()..)
            .take(config.thread_count_per_process())
            .map(|thread_id| {
                let cores = &cores;
                let barrier_thread = &barrier_thread;
                let backend = &backend;
                let global = &global;
                let process = &process;
                let handle = scope.spawn(move || -> anyhow::Result<_> {
                    crate::THREAD_ID.set(Some(thread_id));

                    let config = config::Thread {
                        process: config.clone(),
                        thread_id,
                    };
                    let core = cores[thread_id % cores.len()];

                    let set = unsafe {
                        // Seems like we should use `MaybeUninit<libc::cpu_set_t>`,
                        // but `CPU_ZERO` macro takes `&mut`, not `*mut`.
                        let mut set = mem::zeroed::<libc::cpu_set_t>();
                        libc::CPU_ZERO(&mut set);
                        libc::CPU_SET(core as usize, &mut set);
                        set
                    };

                    log::debug!("Pin thread {} to core {}", thread_id, core);

                    // `hwloc2::Topology::set_cpubind_for_thread` takes `&mut self`,
                    // so just call `sched_setaffinity` ourselves.
                    if unsafe { libc::sched_setaffinity(0, libc::CPU_SETSIZE as usize, &set) } != 0
                    {
                        return Err(io::Error::last_os_error())
                            .with_context(|| anyhow!("sched_setaffinity({})", core));
                    }

                    let mut perf = perf_internal
                        .then(|| measure::Perf::new(core as usize))
                        .transpose()
                        .context("Initialize perf-event")?;

                    let mut allocator = backend.allocator(thread_id);
                    let mut worker =
                        benchmark.setup_worker(&config, global, process, &mut allocator);

                    let _ = barrier_thread.wait()?;
                    let before = measure::Resource::new().context("Get resource usage")?;
                    if let Some(perf) = &mut perf {
                        perf.start().context("Start perf-event")?;
                    }

                    let start = Instant::now();
                    let output =
                        benchmark.run_worker(&config, global, process, &mut worker, &mut allocator);
                    let total = start.elapsed();

                    let report = perf
                        .as_mut()
                        .map(|perf| perf.stop())
                        .transpose()
                        .context("Stop perf-event")?;
                    let after = measure::Resource::new().context("Get resource usage")?;
                    let _ = barrier_thread.wait()?;

                    let allocator = allocator.report();

                    let mut time = measure::time::report();
                    time.total = total.as_nanos() as u64;

                    Ok((thread_id, time, after - before, report, output, allocator))
                });
                handle
            })
            .collect::<Vec<_>>();

        let coordinator = scope.spawn(|| -> anyhow::Result<_> {
            let mut coordinator = benchmark.setup_coordinator(config, &global, &process);

            if let Some(perf) = &mut perf_external {
                perf.enable()?;
            }

            let _ = barrier_thread.wait()?;
            let output = benchmark.run_coordinator(config, &global, &process, &mut coordinator);
            let _ = barrier_thread.wait()?;

            if let Some(perf) = &mut perf_external {
                perf.disable()?;
            }

            Ok(output)
        });

        let output_workers = workers
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<anyhow::Result<Vec<_>>>()
            .unwrap();

        let output_coordinator = coordinator.join().unwrap().unwrap();

        let memory = measure::memory::Total::new(|mapping| backend.categorize(mapping))
            .context("Get memory usage")?;

        let mut stdout = std::io::stdout().lock();
        serde_json::ser::to_writer(
            &mut stdout,
            &Observation {
                date,
                cargo: crate::Cargo::default(),
                r#global: config.global.clone(),
                allocator: serde_json::to_value(allocator).unwrap(),
                benchmark: serde_json::to_value(Named {
                    name: B::NAME,
                    inner: benchmark,
                })
                .unwrap(),
                output: Output {
                    process: OutputProcess {
                        id: config.process_id,
                        memory,
                        output: serde_json::to_value(output_coordinator)?,
                        allocator: backend.report(),
                    },
                    thread: output_workers
                        .into_iter()
                        .map(
                            |(id, time, resource, perf, output, allocator)| OutputThread {
                                id,
                                time,
                                resource,
                                perf,
                                output: serde_json::to_value(output).unwrap(),
                                allocator,
                            },
                        )
                        .collect(),
                },
            },
        )?;

        Ok(())
    })?;

    benchmark.teardown_process(config, &global, process);

    benchmark.teardown_global(config, global);

    if config.is_leader() {
        barrier_thread.unlink()?;
        barrier_process.unlink()?;
        backend.unlink()?;
    }

    Ok(())
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
