// https://github.com/emeryberger/Hoard/blob/f021bdb810332c9c9f5a11ae5404aaa38fe129c0/benchmarks/threadtest/threadtest.cpp

use core::cmp;
use core::mem::MaybeUninit;
use core::num::NonZeroU64;
use core::ptr;
use core::sync::atomic::AtomicU64;
use core::sync::atomic::Ordering;

use bon::Builder;
use rand::RngCore as _;
use rand::SeedableRng as _;
use rand::rngs::SmallRng;
use serde::Deserialize;
use serde::Serialize;
use shm::Shm;

use crate::Allocator;
use crate::allocator;
use crate::allocator::Backend;
use crate::allocator::Handle;
use crate::benchmark;
use crate::config;

#[derive(Builder, Clone, Debug, Deserialize, Serialize)]
pub struct Xmalloc {
    #[builder(default = 100)]
    limit: u64,

    #[builder(default = 120)]
    batch_count: u64,

    #[builder(default = 10_000_000)]
    operation_count: u64,

    #[builder(default = false)]
    huge: bool,
}

const POSSIBLE_SIZES: &[usize] = &[
    8,
    12,
    16,
    24,
    32,
    48,
    64,
    96,
    128,
    192,
    256,
    (256 * 3) / 2,
    512,
    (512 * 3) / 2,
    // 1024,
    // (1024 * 3) / 2,
    // 2048,
];

#[repr(C)]
struct Root {
    lock: libc::pthread_mutex_t,
    empty: libc::pthread_cond_t,
    full: libc::pthread_cond_t,

    operation_count: AtomicU64,
    len: AtomicU64,
    head: AtomicU64,
}

pub struct Global {
    root: Shm<Root>,
}

#[derive(Deserialize, Serialize)]
pub struct OutputWorker {
    operation: Operation,
    operation_count: u64,
    size: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Push,
    Pop,
}

pub struct Worker {
    rng: SmallRng,
    operation_count: u64,
}

unsafe impl Sync for Global {}

impl<B: Backend> benchmark::Benchmark<B> for Xmalloc {
    const NAME: &str = "xmalloc";
    type StateGlobal = Global;
    type StateProcess = ();
    type StateCoordinator = ();
    type StateWorker = Worker;

    type OutputWorker = OutputWorker;
    type OutputCoordinator = ();

    fn setup_global(
        &self,
        config: &config::Process,
        allocator: &allocator::Config<B::Config>,
    ) -> Self::StateGlobal {
        assert!(
            config.thread_count & 1 == 0,
            "Thread count ({}) must be evenly divisible by 2",
            config.thread_count
        );

        let create = config.is_leader();

        let root = Shm::<Root>::builder()
            .create(create)
            .maybe_numa(allocator.numa.clone())
            .name("xmalloc".to_owned())
            .populate(shm::Populate::Physical)
            .build()
            .unwrap();

        if create {
            unsafe {
                let mut attr = MaybeUninit::<libc::pthread_mutexattr_t>::zeroed();
                libc::pthread_mutexattr_init(attr.as_mut_ptr());
                libc::pthread_mutexattr_setpshared(attr.as_mut_ptr(), libc::PTHREAD_PROCESS_SHARED);

                let lock = ptr::addr_of_mut!((*root.address().as_ptr()).lock);
                libc::pthread_mutex_init(lock, attr.as_ptr());

                let mut attr = MaybeUninit::<libc::pthread_condattr_t>::zeroed();
                libc::pthread_condattr_init(attr.as_mut_ptr());
                libc::pthread_condattr_setpshared(attr.as_mut_ptr(), libc::PTHREAD_PROCESS_SHARED);

                let empty = ptr::addr_of_mut!((*root.address().as_ptr()).empty);
                libc::pthread_cond_init(empty, attr.as_ptr());

                let full = ptr::addr_of_mut!((*root.address().as_ptr()).full);
                libc::pthread_cond_init(full, attr.as_ptr());

                ptr::addr_of_mut!((*root.address().as_ptr()).operation_count)
                    .write(AtomicU64::new(self.operation_count));

                ptr::addr_of_mut!((*root.address().as_ptr()).len).write(AtomicU64::new(0));

                ptr::addr_of_mut!((*root.address().as_ptr()).head).write(AtomicU64::new(0));
            }
        }

        Global { root }
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
        _global: &Self::StateGlobal,
        (): &Self::StateProcess,
        _allocator: &mut B::Allocator,
    ) -> Self::StateWorker {
        Worker {
            rng: SmallRng::seed_from_u64(config.thread_id as u64),
            operation_count: self.operation_count / (config.thread_count >> 1) as u64,
        }
    }

    fn run_coordinator(
        &self,
        _config: &config::Process,
        _global: &Self::StateGlobal,
        (): &Self::StateProcess,
        (): &mut Self::StateCoordinator,
    ) -> Self::OutputCoordinator {
    }

    fn run_worker(
        &self,
        config: &config::Thread,
        global: &Self::StateGlobal,
        (): &Self::StateProcess,
        worker: &mut Self::StateWorker,
        allocator: &mut B::Allocator,
    ) -> Self::OutputWorker {
        // Allocator
        let (operation, operation_count, size_total) = if config.thread_id & 1 == 0 {
            let mut size_total = 0;

            for _ in 0..worker.operation_count {
                let batch = allocator
                    .allocate((1 + self.batch_count as usize) * 8)
                    .unwrap();

                for i in 0..self.batch_count as usize {
                    let size = match self.huge {
                        true => 1 << 30,
                        false => {
                            POSSIBLE_SIZES[worker.rng.next_u32() as usize % POSSIBLE_SIZES.len()]
                        }
                    };

                    size_total += size as u64;
                    let object = allocator.allocate(size).unwrap();

                    unsafe {
                        libc::memset(object.as_ptr(), i as u8 as i32, cmp::min(128, size));
                    }

                    unsafe {
                        allocator.link(batch.as_ptr().cast::<u64>().add(1).add(i), &object);
                    }
                }

                global.push(self, allocator, batch);
            }

            (Operation::Push, worker.operation_count, size_total)

        // Releaser
        } else {
            let mut operation_count = 0;
            loop {
                let Some(handle) = global.pop(allocator) else {
                    break;
                };

                let batch = handle.as_ptr().cast::<u64>();

                for i in 0..self.batch_count {
                    unsafe {
                        let offset = batch.add(1).add(i as usize);
                        let handle = allocator.offset_to_handle(NonZeroU64::new(*offset).unwrap());
                        handle.as_ptr().cast::<u64>().write(0xff);
                        allocator.unlink(offset);
                    }
                }

                unsafe {
                    allocator.deallocate(handle);
                }
                operation_count += 1;
            }

            (Operation::Pop, operation_count, 0)
        };

        OutputWorker {
            operation,
            // One operation per object, plus one for the batch container itself
            operation_count: operation_count * (self.batch_count + 1),
            size: size_total,
        }
    }

    fn teardown_global(&self, config: &config::Process, mut global: Self::StateGlobal) {
        if !config.is_leader() {
            return;
        }

        global.root.unlink().unwrap();
    }
}

impl Global {
    fn push<A: Allocator>(&self, config: &Xmalloc, allocator: &mut A, handle: A::Handle) {
        let batch = unsafe { handle.as_ptr().cast::<u64>().as_mut().unwrap() };

        let root = unsafe { self.root.address().as_ref() };

        unsafe {
            libc::pthread_mutex_lock(&root.lock as *const _ as *mut _);
        }

        while root.len.load(Ordering::Relaxed) >= config.limit {
            unsafe {
                libc::pthread_cond_wait(
                    &root.full as *const _ as *mut _,
                    &root.lock as *const _ as *mut _,
                );
            }
        }

        let next = unsafe { AtomicU64::from_ptr(batch) };

        next.store(root.head.load(Ordering::Relaxed), Ordering::Relaxed);

        unsafe {
            allocator.link(root.head.as_ptr(), &handle);
        }

        root.len
            .store(root.len.load(Ordering::Relaxed) + 1, Ordering::Relaxed);

        root.operation_count.store(
            root.operation_count.load(Ordering::Relaxed) - 1,
            Ordering::Relaxed,
        );

        unsafe {
            libc::pthread_cond_signal(&root.empty as *const _ as *mut _);
            libc::pthread_mutex_unlock(&root.lock as *const _ as *mut _);
        }
    }

    fn pop<A: Allocator>(&self, allocator: &mut A) -> Option<A::Handle> {
        let root = unsafe { self.root.address().as_ref() };

        unsafe {
            libc::pthread_mutex_lock(&root.lock as *const _ as *mut _);
        }

        let head = loop {
            match (
                NonZeroU64::new(root.head.load(Ordering::Relaxed)),
                root.operation_count.load(Ordering::Relaxed),
            ) {
                (Some(head), _) => break head,
                (None, 0) => {
                    // TODO: RAII
                    unsafe {
                        libc::pthread_mutex_unlock(&root.lock as *const _ as *mut _);
                    }

                    return None;
                }
                (None, _) => unsafe {
                    libc::pthread_cond_wait(
                        &root.empty as *const _ as *mut _,
                        &root.lock as *const _ as *mut _,
                    );
                },
            }
        };

        let handle = allocator.offset_to_handle(head);
        let pointer = handle.as_ptr();
        let next = unsafe { pointer.cast::<u64>().read() };

        // HACK: only cxl-shm needs to decrement reference count here
        if std::any::type_name::<A>().contains("cxl_shm") {
            unsafe {
                allocator.unlink(root.head.as_ptr());
            }
        }

        root.head.store(next, Ordering::Relaxed);
        root.len
            .store(root.len.load(Ordering::Relaxed) - 1, Ordering::Relaxed);

        match root.operation_count.load(Ordering::Relaxed) {
            // Wake up all readers
            0 => unsafe {
                libc::pthread_cond_broadcast(&root.empty as *const _ as *mut _);
            },
            // Wake up one writer
            _ => unsafe {
                libc::pthread_cond_signal(&root.full as *const _ as *mut _);
            },
        }

        unsafe {
            libc::pthread_mutex_unlock(&root.lock as *const _ as *mut _);
        }

        Some(handle)
    }
}
