use core::hash::Hash;
use core::hash::Hasher as _;
use core::num::NonZeroU64;
use core::num::NonZeroUsize;
use core::ptr;
use core::slice;
use core::sync::atomic::AtomicU64;
use core::sync::atomic::Ordering;

use rapidhash::RapidHasher;
use shm::Shm;

use crate::Allocator;
use crate::Index;
use crate::allocator::Handle as _;
use crate::ebr;
use crate::measure;

/// Separate chaining hashmap
///
/// Inserted nodes are one contiguous allocation with the
/// next pointer, key, and value.
pub struct LinkedHashMap<A> {
    len: usize,
    ebr: Shm<ebr::Global<A>>,
    raw: shm::Raw,
}

impl<A: Allocator> Index<A> for LinkedHashMap<A> {
    fn new(
        numa: Option<shm::Numa>,
        len: usize,
        create: bool,
        populate: Option<shm::Populate>,
        thread_count: usize,
    ) -> anyhow::Result<Self> {
        let ebr = shm::Shm::builder()
            .maybe_numa(numa.clone())
            .name("ebr".to_owned())
            .create(create)
            .maybe_populate(populate)
            .build()?;

        if create {
            unsafe {
                ebr::Global::init(ebr.address().as_ptr(), thread_count);
            }
        }

        Ok(Self {
            len,
            ebr,
            raw: shm::Raw::builder()
                .maybe_numa(numa)
                .name("index".to_owned())
                .size(len * 8)
                .create(create)
                .maybe_populate(populate)
                .build()?,
        })
    }

    fn insert<F: FnOnce(*mut u8)>(
        &self,
        thread_id: usize,
        allocator: &mut A,
        key: &[u8],
        size: usize,
        with: F,
    ) {
        let _timer = measure::time::Timer::start(&measure::time::INDEX);

        let bucket = self.bucket(key);

        // Initialize value
        let handle_value = match NonZeroUsize::new(size) {
            None => None,
            Some(size) => {
                let handle_value = allocator.allocate(size.get()).unwrap();
                with(handle_value.as_ptr().cast::<u8>());
                Some(handle_value)
            }
        };

        // Fast path: swap value in place
        if self.try_swap(
            thread_id,
            allocator,
            bucket.load(Ordering::Relaxed),
            key,
            handle_value.as_ref(),
        ) {
            return;
        }

        // Slow path: allocate node
        let handle_node = allocator.allocate(24).unwrap();
        let offset_node = unsafe { allocator.handle_to_offset(&handle_node) };

        // Initialize key
        let pointer_key = unsafe { handle_node.as_ptr().byte_add(8).cast::<u64>() };
        let handle_key = allocator.allocate(8 + key.len()).unwrap();
        unsafe {
            let pointer_key = handle_key.as_ptr();
            pointer_key.cast::<usize>().write(key.len());
            pointer_key
                .byte_add(8)
                .cast::<u8>()
                .copy_from_nonoverlapping(key.as_ptr(), key.len());
        }
        unsafe {
            allocator.link(pointer_key, &handle_key);
        }

        // Link value into node
        let pointer_value = unsafe { handle_node.as_ptr().byte_add(16).cast::<u64>() };
        match handle_value.as_ref() {
            None => unsafe { pointer_value.write(0) },
            Some(handle) => unsafe { allocator.link(pointer_value, handle) },
        }

        let pointer_next = handle_node.as_ptr().cast::<u64>();

        loop {
            // Must store current head before calling find
            let head = bucket.load(Ordering::Relaxed);
            unsafe { pointer_next.write(head) };

            if self.try_swap(thread_id, allocator, head, key, handle_value.as_ref()) {
                unsafe {
                    if Self::has_reference_count() {
                        allocator.unlink(pointer_key);
                        allocator.unlink(pointer_value);
                    } else {
                        allocator.deallocate(handle_key);
                    }

                    allocator.deallocate(handle_node);
                }
                return;
            }

            // Try to CAS new node at head
            // FIXME: ABA problem
            if bucket
                .compare_exchange(head, offset_node.get(), Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }

            // Increment reference count of node
            if Self::has_reference_count() {
                let mut link = 0;
                unsafe {
                    allocator.link(&mut link, &handle_node);
                }
            }

            return;
        }
    }

    fn get<F: FnOnce(*const u8)>(
        &self,
        thread_id: usize,
        allocator: &mut A,
        key: &[u8],
        with: F,
    ) -> bool {
        let _timer = measure::time::Timer::start(&measure::time::INDEX);

        unsafe {
            self.ebr().start(thread_id, allocator);
        }

        let bucket = self.bucket(key);
        let head = bucket.load(Ordering::Acquire);
        let Some(handle_node) = self.find(allocator, head, key) else {
            return false;
        };

        match NonZeroU64::new(unsafe { handle_node.as_ptr().byte_add(16).cast::<u64>().read() }) {
            None => with(ptr::null()),
            Some(offset) => {
                let handle_value = allocator.offset_to_handle(offset);
                with(handle_value.as_ptr().cast());
            }
        }

        true
    }

    fn unlink(&mut self) -> anyhow::Result<()> {
        self.ebr.unlink()?;
        self.raw.unlink()?;
        Ok(())
    }
}

impl<A: Allocator> LinkedHashMap<A> {
    fn bucket(&self, key: &[u8]) -> &AtomicU64 {
        let mut hasher = RapidHasher::default();
        key.hash(&mut hasher);
        &self.view()[hasher.finish() as usize % self.len]
    }

    fn try_swap(
        &self,
        thread_id: usize,
        allocator: &mut A,
        head: u64,
        key: &[u8],
        value: Option<&A::Handle>,
    ) -> bool {
        let Some(handle_node) = self.find(allocator, head, key) else {
            return false;
        };

        let site = unsafe { AtomicU64::from_ptr(handle_node.as_ptr().byte_add(16).cast::<u64>()) };
        let new = value
            .map(|handle| unsafe { allocator.handle_to_offset(handle) })
            .map(NonZeroU64::get)
            .unwrap_or(0);

        if Self::has_reference_count() {
            // Increment reference count of `new` *before* publishing
            if let Some(value) = value {
                let mut link = 0;
                unsafe {
                    allocator.link(&mut link, value);
                }
            }
        }

        if let Some(old) = NonZeroU64::new(site.swap(new, Ordering::AcqRel)) {
            unsafe { self.ebr().retire(thread_id, allocator, old) }
        }

        true
    }

    fn find(&self, allocator: &mut A, head: u64, key: &[u8]) -> Option<A::Handle> {
        let offset_walk = NonZeroU64::new(head)?;
        let mut handle_walk = allocator.offset_to_handle(offset_walk);

        loop {
            let offset_key =
                NonZeroU64::new(unsafe { handle_walk.as_ptr().byte_add(8).cast::<u64>().read() })
                    .unwrap();

            let handle_key = allocator.offset_to_handle(offset_key);
            let candidate_len = unsafe { handle_key.as_ptr().cast::<usize>().read() };
            let candidate = unsafe {
                slice::from_raw_parts(handle_key.as_ptr().cast::<u8>().byte_add(8), candidate_len)
            };

            if candidate == key {
                return Some(handle_walk);
            }

            let offset_walk =
                NonZeroU64::new(unsafe { handle_walk.as_ptr().cast::<u64>().read() })?;
            handle_walk = allocator.offset_to_handle(offset_walk);
        }
    }

    fn has_reference_count() -> bool {
        std::any::type_name::<A>().contains("cxl_shm")
    }

    fn ebr(&self) -> &ebr::Global<A> {
        unsafe { self.ebr.address().as_ref() }
    }

    fn view(&self) -> &[AtomicU64] {
        unsafe {
            std::slice::from_raw_parts(self.raw.address().cast::<AtomicU64>().as_ptr(), self.len)
        }
    }
}
