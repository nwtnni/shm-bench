// https://github.com/emeryberger/Hoard/blob/f021bdb810332c9c9f5a11ae5404aaa38fe129c0/benchmarks/threadtest/threadtest.cpp

use core::num::NonZeroU64;
use core::ptr::NonNull;
use core::sync::atomic::AtomicU64;
use core::sync::atomic::Ordering;

use bon::Builder;
use rand::Rng as _;
use rand::SeedableRng;
use rand::rngs::SmallRng;
use serde::Deserialize;
use serde::Serialize;

use crate::Allocator;
use crate::allocator;
use crate::allocator::Backend;
use crate::allocator::Handle as _;
use crate::benchmark;
use crate::config;

const COOKIE: usize = 0xbf58476d1ce4e5b9;

#[derive(Builder, Clone, Debug, Deserialize, Serialize)]
pub struct Mstress {}

#[derive(Deserialize, Serialize)]
pub struct OutputWorker {}

impl<B: Backend> benchmark::Benchmark<B> for Mstress {
    const NAME: &str = "mstress";
    type StateGlobal = ();
    type StateProcess = ();
    type StateCoordinator = ();
    type StateWorker = ();

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
    ) -> Self::StateGlobal {
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
        (): &Self::StateGlobal,
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
        _config: &config::Thread,
        (): &Self::StateGlobal,
        (): &Self::StateProcess,
        (): &mut Self::StateWorker,
        allocator: &mut <B as allocator::Backend>::Allocator,
    ) -> Self::OutputWorker {
        let mut allocs = 100 * 50;
        let mut retain = allocs / 2;
        let mut rng = SmallRng::seed_from_u64(0);

        let mut data = Vec::new();
        let mut retained = Vec::new();

        let min_item_shift = 3;
        let max_item_shift = 8;
        let max_item_retained_shift = max_item_shift + 2;

        while allocs > 0 || retain > 0 {
            if retain == 0 || rng.random_bool(0.5) {
                allocs -= 1;

                let size = rng.random_range(min_item_shift..max_item_shift);

                data.push(0);

                let offset = data.last_mut().unwrap();
                let handle = allocate(&mut rng, allocator, 1 << size);

                unsafe {
                    allocator.link(offset, &handle);
                }
            } else {
                retain -= 1;

                let size = rng.random_range(min_item_shift..max_item_retained_shift);

                retained.push(0);

                let offset = retained.last_mut().unwrap();
                let handle = allocate(&mut rng, allocator, 1 << size);

                unsafe {
                    allocator.link(offset, &handle);
                }
            }
        }

        for offset in retained.into_iter().chain(data).filter_map(NonZeroU64::new) {
            let handle = allocator.offset_to_handle(offset);
            free(allocator, handle);
        }

        OutputWorker {}
    }
}

fn allocate<A: Allocator, R: rand::Rng>(
    rng: &mut R,
    allocator: &mut A,
    mut size: usize,
) -> A::Handle {
    if rng.random_bool(0.01) {
        size *= match rng.random_range(0..100) {
            0 => 10_000,
            1..11 => 1_000,
            _ => 100,
        }
    }

    let handle = allocator.allocate(size).unwrap();

    let count = size / 8;
    let pointer = handle.as_ptr().cast::<u64>();
    for index in 0..count {
        unsafe { AtomicU64::from_ptr(pointer.add(index)) }
            .store(((count - index) ^ COOKIE) as u64, Ordering::Relaxed);
    }

    handle
}

fn free<A: Allocator>(allocator: &mut A, handle: A::Handle) {
    let Some(pointer) = NonNull::new(handle.as_ptr().cast::<u64>()) else {
        return;
    };

    let count = (unsafe { AtomicU64::from_ptr(pointer.as_ptr()) }.load(Ordering::Relaxed) as usize)
        ^ COOKIE;
    for index in 0..count {
        if unsafe {
            AtomicU64::from_ptr(pointer.add(index).as_ptr()).load(Ordering::Relaxed) as usize
        } ^ COOKIE
            != count - index
        {
            panic!();
        }
    }

    unsafe { allocator.deallocate(handle) }
}
