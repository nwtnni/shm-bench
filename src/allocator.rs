use core::ffi;
use core::num::NonZeroU64;
use core::ptr::NonNull;
use core::sync::atomic::AtomicU64;
use core::sync::atomic::Ordering;

use bon::Builder;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::Mapping;

#[derive(Builder, Clone, Debug, Deserialize, Serialize)]
#[builder(state_mod(vis = "pub", name = "config"), derive(Clone, Debug))]
pub struct Config<T> {
    pub name: String,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub numa: Option<shm::Numa>,

    /// Initial heap size
    pub size: usize,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub populate: Option<shm::Populate>,

    pub consistency: Consistency,
    pub coherence: Coherence,

    #[serde(default)]
    #[serde(flatten)]
    pub inner: T,
}

impl<T> Config<T> {
    pub fn map<F: FnOnce(&T) -> U, U>(&self, apply: F) -> Config<U> {
        Config {
            name: self.name.clone(),
            numa: self.numa.clone(),
            size: self.size,
            populate: self.populate,
            consistency: self.consistency,
            coherence: self.coherence,
            inner: apply(&self.inner),
        }
    }
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Consistency {
    None,

    /// Only sfence
    Sfence,

    /// Only clflush
    Clflush,

    /// (clflush or clwb) and sfence
    Clflushopt,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Coherence {
    None,

    /// Limited region of hardware cache coherence
    Limited,

    /// No hardware cache coherence, MCAS only
    Mcas,
}

pub trait Backend: Sync + Sized {
    type Allocator: Allocator;
    type Config: DeserializeOwned + Serialize;

    fn new(create: bool, config: &Config<Self::Config>, name: &str) -> anyhow::Result<Self>;

    fn unlink(self) -> anyhow::Result<()>;
    fn allocator(&self, thread_id: usize) -> Self::Allocator;

    fn contains(&self, mapping: &Mapping) -> bool;

    fn report(&self) -> serde_json::Value {
        serde_json::Value::Null
    }
}

pub trait Allocator: Sized {
    type Handle: Handle;

    fn allocate(&mut self, size: usize) -> Option<Self::Handle>;

    unsafe fn link(&mut self, pointer: *mut u64, pointee: &Self::Handle) {
        unsafe {
            let offset = self.handle_to_offset(pointee);
            AtomicU64::from_ptr(pointer).store(offset.get(), Ordering::Release);
        }
    }

    unsafe fn unlink(&mut self, pointer: *mut u64) {
        let offset = unsafe { AtomicU64::from_ptr(pointer) }.load(Ordering::Relaxed);
        let Some(offset) = NonZeroU64::new(offset) else {
            return;
        };
        let handle = self.offset_to_handle(offset);
        unsafe { self.deallocate(handle) }
    }

    unsafe fn deallocate(&mut self, handle: Self::Handle);

    unsafe fn handle_to_offset(&mut self, handle: &Self::Handle) -> NonZeroU64;
    fn offset_to_handle(&mut self, offset: NonZeroU64) -> Self::Handle;
    fn pointer_to_offset(&self, pointer: NonNull<ffi::c_void>) -> NonZeroU64;

    fn report(&self) -> serde_json::Value {
        serde_json::Value::Null
    }
}

pub trait Handle {
    fn as_ptr(&self) -> *mut ffi::c_void;
}

impl Handle for *mut ffi::c_void {
    fn as_ptr(&self) -> *mut ffi::c_void {
        *self
    }
}

impl Handle for NonNull<ffi::c_void> {
    fn as_ptr(&self) -> *mut ffi::c_void {
        (*self).as_ptr()
    }
}
