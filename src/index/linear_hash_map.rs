use core::hash::Hash;
use core::hash::Hasher as _;
use core::sync::atomic::AtomicU64;

use rapidhash::RapidHasher;

use crate::Allocator;
use crate::Index;

// Open-addressed, linear probing hashmap.
pub struct LinearHashMap {
    len: usize,
    raw: shm::Raw,
}

impl<A: Allocator> Index<A> for LinearHashMap {
    fn new(
        _numa: Option<shm::Numa>,
        _len: usize,
        _create: bool,
        _populate: Option<shm::Populate>,
        _thread_count: usize,
    ) -> anyhow::Result<Self> {
        todo!()
    }

    fn insert<F: FnOnce(*mut u8)>(
        &self,
        _thread_id: usize,
        _allocator: &mut A,
        _key: &[u8],
        _size: usize,
        _with: F,
    ) {
        todo!()
    }

    fn get<F: FnOnce(*const u8)>(
        &self,
        _thread_id: usize,
        _allocator: &mut A,
        _key: &[u8],
        _with: F,
    ) -> bool {
        todo!()
    }

    fn unlink(&mut self) -> anyhow::Result<()> {
        self.raw.unlink().map_err(anyhow::Error::from)
    }
}

impl LinearHashMap {
    #[expect(dead_code)]
    fn index<K: Hash>(&self, key: &K) -> usize {
        let mut hasher = RapidHasher::default();
        key.hash(&mut hasher);
        hasher.finish() as usize % self.len
    }

    #[expect(dead_code)]
    fn view(&self) -> &[AtomicU64] {
        unsafe {
            std::slice::from_raw_parts(self.raw.address().cast::<AtomicU64>().as_ptr(), self.len)
        }
    }
}
