use core::ops::AddAssign;
use std::path::Path;

use crate::Mapping;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Default, Deserialize, Serialize)]
pub struct Total {
    hwcc: Memory,
    swcc: Memory,
}

#[derive(Clone, Default, Deserialize, Serialize)]
pub struct Memory {
    pss: u64,
    pss_dirty: u64,
    shared_clean: u64,
    shared_dirty: u64,
    private_clean: u64,
    private_dirty: u64,
}

impl Total {
    pub fn new<F: Fn(&Mapping) -> Option<crate::allocator::Memory>>(
        filter: F,
    ) -> anyhow::Result<Self> {
        let mut total = Total::default();

        let mut parser = smaps::Parser::open(Path::new("/proc/self/smaps"))?;

        loop {
            let (parse_usage, Some(mapping)) = parser.next()? else {
                break;
            };

            let Some(category) = filter(&mapping) else {
                parser = parse_usage.skip();
                continue;
            };

            let (parse_mapping, usage) = parse_usage.next()?;
            let usage = usage.expect("Failed to parse usage");

            match category {
                crate::allocator::Memory::Hwcc => total.hwcc += usage,
                crate::allocator::Memory::Swcc => total.swcc += usage,
            }

            parser = parse_mapping;
        }

        Ok(total)
    }
}

impl AddAssign<smaps::Usage> for Memory {
    fn add_assign(&mut self, usage: smaps::Usage) {
        self.pss += usage.pss as u64;
        self.pss_dirty += usage.pss_dirty as u64;
        self.shared_clean += usage.shared_clean as u64;
        self.shared_dirty += usage.shared_dirty as u64;
        self.private_clean += usage.private_clean as u64;
        self.private_dirty += usage.private_dirty as u64;
    }
}
