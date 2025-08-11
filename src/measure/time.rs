use core::cell::Cell;
use core::ffi;
use core::num::NonZeroU64;
use core::ptr::NonNull;
use std::thread::LocalKey;

use serde::Deserialize;
use serde::Serialize;

thread_local! {
    pub(crate) static INDEX: Count = const { Count::new() };
    pub(crate) static ALLOCATOR: Count = const { Count::new() };
    pub(crate) static EBR: Count = const { Count::new() };
}

#[derive(Serialize, Deserialize)]
pub struct Report {
    pub(crate) total: u64,
    index: u64,
    allocator: u64,
    ebr: u64,
}

pub fn report() -> Report {
    Report {
        total: 0,
        index: INDEX.with(Count::get),
        allocator: ALLOCATOR.with(Count::get),
        ebr: EBR.with(Count::get),
    }
}

pub struct Timer {
    #[cfg(feature = "measure-time")]
    start: std::time::Instant,
    #[cfg(feature = "measure-time")]
    count: &'static LocalKey<Count>,
}

impl Timer {
    #[inline]
    pub(crate) fn start(_count: &'static LocalKey<Count>) -> Self {
        #[cfg(feature = "measure-time")]
        {
            let start = std::time::Instant::now();
            Self {
                start,
                count: _count,
            }
        }

        #[cfg(not(feature = "measure-time"))]
        Self {}
    }
}

#[cfg(feature = "measure-time")]
impl Drop for Timer {
    #[inline]
    fn drop(&mut self) {
        let time = self.start.elapsed();
        self.count
            .with(|count| count.0.set(count.get() + time.as_nanos() as u64));
    }
}

pub(crate) struct Count(Cell<u64>);

impl Count {
    const fn new() -> Self {
        Count(Cell::new(0))
    }

    fn get(&self) -> u64 {
        self.0.get()
    }
}

pub struct Backend<B>(B);

impl<B> Backend<B> {
    pub fn new(backend: B) -> Self {
        Self(backend)
    }
}

impl<B: crate::allocator::Backend> crate::allocator::Backend for Backend<B> {
    type Allocator = Allocator<B::Allocator>;

    type Config = B::Config;

    #[inline]
    fn new(
        create: bool,
        config: &crate::allocator::Config<Self::Config>,
        name: &str,
    ) -> anyhow::Result<Self> {
        B::new(create, config, name).map(Self)
    }

    #[inline]
    fn unlink(self) -> anyhow::Result<()> {
        self.0.unlink()
    }

    #[inline]
    fn allocator(&self, thread_id: usize) -> Self::Allocator {
        Allocator(self.0.allocator(thread_id))
    }

    #[inline]
    fn categorize(&self, mapping: &smaps::Mapping) -> Option<crate::allocator::Memory> {
        self.0.categorize(mapping)
    }
}

pub struct Allocator<A>(A);

impl<A: crate::allocator::Allocator> crate::allocator::Allocator for Allocator<A> {
    type Handle = A::Handle;

    #[inline]
    fn allocate(&mut self, size: usize) -> Option<Self::Handle> {
        let _timer = Timer::start(&ALLOCATOR);
        self.0.allocate(size)
    }

    #[inline]
    unsafe fn deallocate(&mut self, handle: Self::Handle) {
        let _timer = Timer::start(&ALLOCATOR);
        unsafe { self.0.deallocate(handle) }
    }

    #[inline]
    unsafe fn handle_to_offset(&mut self, handle: &Self::Handle) -> NonZeroU64 {
        let _timer = Timer::start(&ALLOCATOR);
        unsafe { self.0.handle_to_offset(handle) }
    }

    #[inline]
    fn offset_to_handle(&mut self, offset: NonZeroU64) -> Self::Handle {
        let _timer = Timer::start(&ALLOCATOR);
        self.0.offset_to_handle(offset)
    }

    #[inline]
    fn pointer_to_offset(&self, pointer: NonNull<ffi::c_void>) -> NonZeroU64 {
        let _timer = Timer::start(&ALLOCATOR);
        self.0.pointer_to_offset(pointer)
    }

    #[inline]
    unsafe fn link(&mut self, pointer: *mut u64, pointee: &Self::Handle) {
        let _timer = Timer::start(&ALLOCATOR);
        unsafe { self.0.link(pointer, pointee) }
    }

    #[inline]
    unsafe fn unlink(&mut self, pointer: *mut u64) {
        let _timer = Timer::start(&ALLOCATOR);
        unsafe { self.0.unlink(pointer) }
    }

    fn report(&self) -> serde_json::Value {
        self.0.report()
    }
}
