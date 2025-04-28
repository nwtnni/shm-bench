use std::fs::File;
use std::io::Read as _;
use std::io::Write as _;
use std::path::Path;

pub(crate) struct Perf {
    ctl: File,
    ack: File,
}

impl Perf {
    pub fn new<C: AsRef<Path>, A: AsRef<Path>>(ctl: C, ack: A) -> Self {
        let ctl = ctl.as_ref();
        let ack = ack.as_ref();

        Self {
            ctl: File::options()
                .write(true)
                .open(ctl)
                .unwrap_or_else(|error| {
                    panic!(
                        "Failed to open perf control file {}: {}",
                        ctl.display(),
                        error,
                    )
                }),
            ack: File::options()
                .read(true)
                .open(ack)
                .unwrap_or_else(|error| {
                    panic!("Failed to open perf ack file {}: {}", ack.display(), error,)
                }),
        }
    }

    pub fn enable(&mut self) {
        self.ctl
            .write_all(b"enable\n\0")
            .expect("Failed to write to perf ctl file");
        self.wait();
    }

    pub fn disable(&mut self) {
        self.ctl
            .write_all(b"disable\n\0")
            .expect("Failed to write to perf ctl file");
        self.wait();
    }

    fn wait(&mut self) {
        match self.ack.read_exact(&mut [0u8; 5]) {
            Ok(()) => (),
            Err(error) => panic!("Failed to read from perf ack file: {}", error),
        }
    }
}
