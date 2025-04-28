use std::path::Path;

use crate::Mapping;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Default, Deserialize, Serialize)]
pub struct Memory {
    pss: u64,
    pss_dirty: u64,
    shared_clean: u64,
    shared_dirty: u64,
    private_clean: u64,
    private_dirty: u64,
}

impl Memory {
    pub fn new<F: Fn(&Mapping) -> bool>(filter: F) -> anyhow::Result<Self> {
        let mut total = Memory::default();
        smaps::read_filter(Path::new("/proc/self/smaps"), filter)?
            .into_iter()
            .for_each(|(_, usage)| {
                total.pss += usage.pss as u64;
                total.pss_dirty += usage.pss_dirty as u64;
                total.shared_clean += usage.shared_clean as u64;
                total.shared_dirty += usage.shared_dirty as u64;
                total.private_clean += usage.private_clean as u64;
                total.private_dirty += usage.private_dirty as u64;
            });

        Ok(total)
    }
}
