use core::any;
use core::cmp;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::cast::AsArray as _;
use arrow_array::types::UInt64Type;
use bon::Builder;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic;
use parquet::schema::types::SchemaDescriptor;
use parquet::schema::types::Type;
use serde::Deserialize;
use serde::Serialize;

use crate::Index;
use crate::allocator;
use crate::allocator::Backend;
use crate::benchmark;
use crate::config;
use crate::index;

#[derive(Builder, Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    pub index: index::Config,

    operation_count: u64,

    pub trace: PathBuf,
}

// HACK: CXL-SHM doesn't support allocations larger than 1KiB (1_000B data + 24B header)
#[expect(unused)]
const MAX_SIZE: usize = 1_000;

pub struct Global<I> {
    index: I,

    mask: ProjectionMask,

    metadata: ArrowReaderMetadata,
}

unsafe impl<I> Sync for Global<I> {}

pub struct Worker {
    operation_count: u64,
    reader: ParquetRecordBatchReader,
}

#[derive(Deserialize, Serialize)]
pub struct OutputWorker {
    time: u128,
    operation_count: u64,
}

impl<B: Backend, I: Index<B::Allocator>> benchmark::Benchmark<B> for index::Capture<Config, I> {
    const NAME: &str = "/mc";
    type StateGlobal = Global<I>;
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
        assert_eq!(
            self.operation_count % config.thread_count as u64,
            0,
            "Operation count ({}) must be evenly divisible by thread count ({})",
            self.operation_count,
            config.thread_count
        );

        let file = File::open(&self.trace).unwrap();
        let schema = SchemaDescriptor::new(Arc::new(
            Type::group_type_builder("trace_schema")
                .with_fields(vec![
                    Arc::new(
                        Type::primitive_type_builder("timestamp", basic::Type::INT64)
                            .with_converted_type(basic::ConvertedType::UINT_64)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        Type::primitive_type_builder("key_value", basic::Type::BYTE_ARRAY)
                            .with_logical_type(Some(basic::LogicalType::String))
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        Type::primitive_type_builder("key_size", basic::Type::INT64)
                            .with_converted_type(basic::ConvertedType::UINT_64)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        Type::primitive_type_builder("value_size", basic::Type::INT64)
                            .with_converted_type(basic::ConvertedType::UINT_64)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        Type::primitive_type_builder("client_id", basic::Type::INT64)
                            .with_converted_type(basic::ConvertedType::UINT_64)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        Type::primitive_type_builder("operation", basic::Type::BYTE_ARRAY)
                            .with_logical_type(Some(basic::LogicalType::String))
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        Type::primitive_type_builder("ttl", basic::Type::INT64)
                            .with_converted_type(basic::ConvertedType::UINT_64)
                            .build()
                            .unwrap(),
                    ),
                ])
                .build()
                .unwrap(),
        ));

        let mask = ProjectionMask::columns(&schema, ["key_value", "value_size", "operation"]);

        let metadata =
            ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true))
                .unwrap();
        Global {
            index: I::new(
                allocator.numa.clone(),
                self.index.len,
                config.is_leader(),
                self.index.populate,
                config.thread_count,
            )
            .unwrap(),
            mask,
            metadata,
        }
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
        global: &Self::StateGlobal,
        (): &Self::StateProcess,
        _allocator: &mut B::Allocator,
    ) -> Self::StateWorker {
        let limit = self.operation_count as usize / config.thread_count;
        let offset = limit * config.thread_id;
        let file = File::open(&self.trace).unwrap();
        let reader =
            ParquetRecordBatchReaderBuilder::new_with_metadata(file, global.metadata.clone())
                .with_offset(offset)
                .with_limit(limit)
                .with_projection(global.mask.clone())
                .build()
                .unwrap();
        Worker {
            operation_count: limit as u64,
            reader,
        }
    }

    fn run_coordinator(
        &self,
        _config: &config::Process,
        _global: &Self::StateGlobal,
        (): &Self::StateProcess,
        _coordinator: &mut Self::StateCoordinator,
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
        let start = Instant::now();

        for batch in &mut worker.reader {
            let batch = batch.unwrap();
            for ((key, value_size), operation) in batch
                .column(0)
                .as_string::<i32>()
                .iter()
                .flatten()
                .zip(
                    batch
                        .column(1)
                        .as_primitive::<UInt64Type>()
                        .iter()
                        .flatten(),
                )
                .zip(batch.column(2).as_string::<i32>().iter().flatten())
            {
                match operation {
                    "get" => {
                        global
                            .index
                            .get(config.thread_id, allocator, key.as_bytes(), |pointer| {
                                if pointer.is_null() {
                                    return;
                                }

                                let size = unsafe { pointer.cast::<u64>().read() };
                                let value = unsafe {
                                    core::slice::from_raw_parts(pointer.byte_add(8), size as usize)
                                };

                                assert!(value.iter().all(|byte| *byte == 0xff));
                            });
                    }
                    "set" if value_size == 0 => {
                        global
                            .index
                            .insert(config.thread_id, allocator, key.as_bytes(), 0, |_| ())
                    }
                    "set" => {
                        let value_size = if any::type_name::<B>().contains("cxl_shm") {
                            cmp::min(value_size, 992)
                        } else {
                            value_size
                        };

                        global.index.insert(
                            config.thread_id,
                            allocator,
                            key.as_bytes(),
                            8 + value_size as usize,
                            |pointer| unsafe {
                                pointer.cast::<u64>().write(value_size);
                                libc::memset(pointer.byte_add(8).cast(), 0xff, value_size as usize);
                            },
                        )
                    }
                    _ => unreachable!(),
                }
            }
        }

        OutputWorker {
            time: start.elapsed().as_nanos(),
            operation_count: worker.operation_count,
        }
    }

    fn teardown_global(&self, config: &config::Process, mut global: Self::StateGlobal) {
        if !config.is_leader() {
            return;
        }

        global.index.unlink().unwrap();
    }
}
