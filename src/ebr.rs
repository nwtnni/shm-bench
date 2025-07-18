use core::cell::UnsafeCell;
use core::ffi;
use core::marker::PhantomData;
use core::mem;
use core::num::NonZeroIsize;
use core::num::NonZeroU64;
use core::ops::Deref;
use core::ptr::NonNull;
use core::ptr::addr_of_mut;
use core::sync::atomic::AtomicIsize;
use core::sync::atomic::AtomicU64;
use core::sync::atomic::AtomicUsize;
use core::sync::atomic::Ordering;

use crate::Allocator;
use crate::allocator::Handle as _;
use crate::measure;

pub struct Global<A> {
    thread_count: usize,

    local: [Pad<UnsafeCell<Local<A>>>; 128],

    // FIXME: replace with cache-line padded boolean array if highly contended
    token: AtomicUsize,
}

impl<A: Allocator> Global<A> {
    pub unsafe fn init(global: *mut Self, thread_count: usize) {
        unsafe {
            AtomicUsize::from_ptr(addr_of_mut!((*global).thread_count))
                .store(thread_count, Ordering::Relaxed);
        }
    }

    pub unsafe fn start(&self, thread_id: usize, allocator: &mut A) {
        let _timer = measure::time::Timer::start(&measure::time::EBR);

        let local = unsafe { self.local[thread_id].get().as_mut().unwrap() };

        if self.try_pass(thread_id) {
            local.rotate();
        }

        local.pop(allocator);
    }

    pub unsafe fn retire(&self, thread_id: usize, allocator: &mut A, offset: NonZeroU64) {
        let _timer = measure::time::Timer::start(&measure::time::EBR);

        let local = unsafe { self.local[thread_id].get().as_mut().unwrap() };
        local.push(allocator, offset);
    }

    fn try_pass(&self, thread_id: usize) -> bool {
        match self.token.load(Ordering::Relaxed) {
            token if token % self.thread_count != thread_id => false,
            token => {
                self.token.store(token + 1, Ordering::Relaxed);
                true
            }
        }
    }
}

struct Local<A> {
    dirty: bool,
    new: Stack<A>,
    old: Stack<A>,
    free: Stack<A>,
}

impl<A: Allocator> Local<A> {
    fn rotate(&mut self) {
        // Since `transfer` only operates on full blocks, we can skip
        // swapping and transferring until we have at least one block.
        if !mem::take(&mut self.dirty) {
            return;
        }

        Stack::transfer(&mut self.old, &mut self.free);
        Stack::swap(&mut self.new, &mut self.old);

        #[cfg(feature = "validate")]
        {
            self.new.invariant();
            self.old.invariant();
            self.free.invariant();
        }
    }

    fn push(&mut self, allocator: &mut A, offset: NonZeroU64) {
        self.dirty = self.new.push_allocation(allocator, offset);

        #[cfg(feature = "validate")]
        {
            self.new.invariant();
        }
    }

    fn pop(&mut self, allocator: &mut A) {
        self.free.pop_allocation(allocator);

        #[cfg(feature = "validate")]
        {
            self.free.invariant();
        }
    }
}

struct Stack<A> {
    #[cfg(feature = "validate")]
    block_count: usize,

    #[cfg(feature = "validate")]
    allocation_count: usize,

    head: Option<Ptr<Block>>,
    tail: Option<Ptr<Block>>,

    _allocator: PhantomData<fn() -> A>,
}

impl<A: Allocator> Stack<A> {
    fn swap(source: &mut Self, dest: &mut Self) {
        // Need to do pointer conversions here because we're using offset pointers
        // Note: in theory this could be optimized to add/subtract the appropriate
        // offset (mem::size_of::<Stack>() assuming contiguous struct layout) when
        // moving pointers between `old` and `new`.
        let source_head = source.head().map(NonNull::from);
        let dest_tail = source.tail().map(NonNull::from);

        source.head.store(dest.head().map(NonNull::from));
        source.tail.store(dest.tail().map(NonNull::from));

        dest.head.store(source_head);
        dest.tail.store(dest_tail);

        #[cfg(feature = "validate")]
        {
            mem::swap(&mut source.block_count, &mut dest.block_count);
            mem::swap(&mut source.allocation_count, &mut dest.allocation_count);
        }
    }

    fn transfer(source: &mut Self, dest: &mut Self) {
        let Some(dest_tail) = &dest.tail else {
            // Simple case: dest is empty
            dest.head.store(source.head().map(NonNull::from));
            dest.tail.store(source.tail().map(NonNull::from));

            source.head.take();
            source.tail.take();

            #[cfg(feature = "validate")]
            {
                dest.block_count = source.block_count;
                dest.allocation_count = source.allocation_count;

                source.block_count = 0;
                source.allocation_count = 0;
            }

            return;
        };

        // Step 1: Initial state
        //
        //   dest.head                     dest.tail
        // ┌───────────┐                 ┌───────────┐
        // │ dest_head ├─►     ...     ─►│ dest_tail │
        // └───────────┘                 └───────────┘
        // ┌───────────┐  ┌───────────┐                 ┌───────────┐
        // │source_head│─►│source_next├─►     ...     ─►│           │
        // └───────────┘  └───────────┘                 └───────────┘
        //  source.head                                  source.tail
        let Some(source_head) = source.head() else {
            return;
        };
        let Some(source_next) = source_head.next() else {
            return;
        };

        // Step 2: Link blocks together
        //
        //   dest.head                     dest.tail
        // ┌───────────┐                 ┌───────────┐
        // │ dest_head ├─►     ...     ─►│ dest_tail │
        // └───────────┘                 └─────┬─────┘
        //                      ┌──────────────┘
        //                      ▼
        // ┌───────────┐  ┌───────────┐                 ┌───────────┐
        // │source_head│  │source_next├─►     ...     ─►│           │
        // └───────────┘  └───────────┘                 └───────────┘
        //  source.head                                  source.tail
        dest_tail.next.store(Some(source_next), Ordering::Relaxed);
        source_head.next.store(None, Ordering::Relaxed);

        // Step 3: Update stack head and tail
        //
        //   dest.head
        // ┌───────────┐                 ┌───────────┐
        // │ dest_head ├─►     ...     ─►│ dest_tail │
        // └───────────┘                 └─────┬─────┘
        //                      ┌──────────────┘
        //                      ▼
        // ┌───────────┐  ┌───────────┐                 ┌───────────┐
        // │source_head│  │source_next├─►     ...     ─►│           │
        // └───────────┘  └───────────┘                 └───────────┘
        //  source.head                                   dest.tail
        //  source.tail
        dest.tail.store(source.tail().map(NonNull::from));
        source.tail.store(source.head().map(NonNull::from));

        #[cfg(feature = "validate")]
        {
            let block_delta = source.block_count.checked_sub(1).unwrap();
            dest.block_count += block_delta;
            source.block_count -= block_delta;

            let allocation_delta = source
                .allocation_count
                .checked_sub(source.head().unwrap().len())
                .unwrap();
            dest.allocation_count += allocation_delta;
            source.allocation_count -= allocation_delta;
        }
    }

    fn push_allocation(&mut self, allocator: &mut A, offset: NonZeroU64) -> bool {
        let dirty = match self.head() {
            Some(head) if head.push(offset) => false,
            None | Some(_) => {
                let head = self.push_block(allocator);
                assert!(head.push(offset));
                true
            }
        };

        #[cfg(feature = "validate")]
        {
            self.allocation_count += 1;
        }

        dirty
    }

    fn push_block(&mut self, allocator: &mut A) -> &Block {
        let handle = allocator.allocate(mem::size_of::<Block>()).unwrap();

        // Initialize block
        let pointer = NonNull::new(handle.as_ptr().cast::<Block>()).unwrap();
        unsafe {
            libc::memset(
                pointer.as_ptr().cast::<ffi::c_void>(),
                0,
                mem::size_of::<Block>(),
            );

            pointer.as_ref().next.store(self.head(), Ordering::Relaxed);
        }

        // Update stack
        self.head.store(Some(pointer));
        if self.tail.is_none() {
            self.tail.store(Some(pointer));
        }

        if Self::has_reference_count() {
            let mut link = 0;
            unsafe {
                allocator.link(&mut link, &handle);
            }
        }

        #[cfg(feature = "validate")]
        {
            self.block_count += 1;
        }

        unsafe { pointer.as_ref() }
    }

    fn pop_allocation(&mut self, allocator: &mut A) {
        let Some(head) = self.head.as_ref() else {
            return;
        };

        match head.pop(allocator) {
            None => unreachable!(),
            Some(false) => {}
            Some(true) => self.pop_block(allocator),
        }

        #[cfg(feature = "validate")]
        {
            self.allocation_count -= 1;
        }
    }

    fn pop_block(&mut self, allocator: &mut A) {
        let Some(head) = self.head() else {
            return;
        };

        // Pop and free empty head block
        let next = head.next().map(NonNull::from);
        let head = NonNull::from(head).cast::<ffi::c_void>();
        self.head.store(next);
        if next.is_none() {
            self.tail.store(None);
        }

        let mut offset = allocator.pointer_to_offset(head).get();
        unsafe {
            allocator.unlink(&mut offset);
        }

        #[cfg(feature = "validate")]
        {
            self.block_count -= 1;
        }
    }

    #[cfg(feature = "validate")]
    fn invariant(&self) {
        use core::ptr;

        let Some((head, tail)) = self.head().zip(self.tail()) else {
            assert!(self.head.is_none());
            assert!(self.tail.is_none());
            assert_eq!(self.block_count, 0);
            assert_eq!(self.allocation_count, 0);
            return;
        };

        if self.block_count == 1 {
            assert!(ptr::eq(head, tail));
        }

        let mut allocation_count = 0;
        let block_count = head
            .walk()
            .inspect(|block| block.invariant())
            .inspect(|block| allocation_count += block.len())
            // Only the head block may be non-full
            .inspect(|block| assert!(ptr::eq(*block, head) || block.len() == Block::LEN))
            .count();

        assert_eq!(block_count, self.block_count);
        assert_eq!(allocation_count, self.allocation_count);
        assert_eq!(tail.walk().count(), 1);
    }

    fn head(&self) -> Option<&Block> {
        self.head.as_deref()
    }

    fn tail(&self) -> Option<&Block> {
        self.tail.as_deref()
    }

    fn has_reference_count() -> bool {
        std::any::type_name::<A>().contains("cxl_shm")
    }
}

struct Block {
    next: AtomicPtr<Self>,
    len: AtomicUsize,
    data: [AtomicU64; Self::LEN],
}

const _: () = assert!(mem::size_of::<Block>() == 512);

impl Block {
    const LEN: usize = 62;

    fn push(&self, offset: NonZeroU64) -> bool {
        let index = match self.len() {
            index @ 0..Self::LEN => index,
            Self::LEN => return false,
            _ => unreachable!(),
        };

        self.data[index].store(offset.get(), Ordering::Relaxed);
        self.len.store(index + 1, Ordering::Relaxed);
        true
    }

    fn pop<A: Allocator>(&self, allocator: &mut A) -> Option<bool> {
        let index = self.len().checked_sub(1)?;

        unsafe {
            allocator.unlink(self.data[index].as_ptr());
        }

        // Can allow `push` to clobber if not validating
        if cfg!(feature = "validate") {
            self.data[index].store(0, Ordering::Relaxed);
        }

        self.len.store(index, Ordering::Relaxed);
        Some(index == 0)
    }

    fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    fn next(&self) -> Option<&Self> {
        self.next.load(Ordering::Relaxed)
    }

    #[cfg(feature = "validate")]
    fn walk(&self) -> impl Iterator<Item = &Self> {
        let mut walk = Some(self);
        core::iter::from_fn(move || {
            let here = walk;
            if let Some(here) = walk {
                walk = here.next();
            }
            here
        })
    }

    #[cfg(feature = "validate")]
    fn invariant(&self) {
        let len = self.len.load(Ordering::Relaxed);

        assert!(len > 0 && len <= Self::LEN);

        for offset in self.data.iter().take(len) {
            assert_ne!(offset.load(Ordering::Relaxed), 0);
        }

        for offset in self.data.iter().skip(len) {
            assert_eq!(offset.load(Ordering::Relaxed), 0);
        }
    }
}

struct AtomicPtr<T> {
    offset: AtomicIsize,
    _type: PhantomData<T>,
}

impl<T> AtomicPtr<T> {
    fn load(&self, ordering: Ordering) -> Option<&T> {
        let offset = NonZeroIsize::new(self.offset.load(ordering))?;
        let base = self as *const Self;
        unsafe { base.byte_offset(offset.get()).cast::<T>().as_ref() }
    }

    fn store(&self, address: Option<&T>, ordering: Ordering) {
        let offset = match address {
            None => 0,
            Some(address) => {
                let address = address as *const T;
                unsafe { address.byte_offset_from(self) }
            }
        };

        self.offset.store(offset, ordering);
    }
}

#[repr(align(64))]
struct Pad<T>(T);

impl<T> Deref for Pad<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

struct Ptr<T> {
    offset: NonZeroIsize,
    _type: PhantomData<T>,
}

trait PtrStore<T> {
    fn store(&mut self, address: Option<NonNull<T>>);
}

impl<T> PtrStore<T> for Option<Ptr<T>> {
    fn store(&mut self, address: Option<NonNull<T>>) {
        *self = match address {
            None => None,
            Some(address) => NonZeroIsize::new(unsafe { address.as_ptr().byte_offset_from(self) })
                .map(|offset| Ptr {
                    offset,
                    _type: PhantomData,
                }),
        };
    }
}

impl<T> Deref for Ptr<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        let base = self as *const Self;
        let offset = self.offset.get();
        unsafe {
            base.byte_offset(offset)
                .cast::<Self::Target>()
                .as_ref()
                .unwrap()
        }
    }
}
