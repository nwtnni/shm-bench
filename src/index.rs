mod linear_hash_map;
mod linked_hash_map;

use bon::Builder;
pub use linear_hash_map::LinearHashMap;
pub use linked_hash_map::LinkedHashMap;

use core::marker::PhantomData;
use core::ops::Deref;

use serde::Deserialize;
use serde::Serialize;

use crate::Allocator;

#[derive(Serialize)]
pub struct Capture<T, I> {
    #[serde(flatten)]
    inner: T,

    #[serde(skip)]
    _index: PhantomData<fn() -> I>,
}

impl<T, I> Capture<T, I> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            _index: PhantomData,
        }
    }
}

impl<T, I> Deref for Capture<T, I> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Builder, Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    pub name: String,

    /// Size of hash map backing array
    pub(crate) len: usize,

    /// Whether to map populate the index
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) populate: Option<shm::Populate>,
}

pub trait Index<A>
where
    Self: Sized,
    A: Allocator,
{
    fn new(
        numa: Option<shm::Numa>,
        len: usize,
        create: bool,
        populate: Option<shm::Populate>,
        thread_count: usize,
    ) -> anyhow::Result<Self>;

    fn unlink(&mut self) -> anyhow::Result<()>;

    fn insert<F: FnOnce(*mut u8)>(
        &self,
        thread_id: usize,
        allocator: &mut A,
        key: &[u8],
        size: usize,
        with: F,
    );

    fn get<F: FnOnce(*const u8)>(
        &self,
        thread_id: usize,
        allocator: &mut A,
        key: &[u8],
        with: F,
    ) -> bool;
}
