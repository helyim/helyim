use std::{
    ffi::c_int,
    fs::{create_dir_all, File},
    path::Path,
};

use criterion::profiler::Profiler;
use pprof::ProfilerGuard;

pub struct FlameGraphProfiler<'a> {
    frequency: c_int,
    active_profiler: Option<ProfilerGuard<'a>>,
}

impl FlameGraphProfiler<'_> {
    pub fn new(frequency: c_int) -> Self {
        FlameGraphProfiler {
            frequency,
            active_profiler: None,
        }
    }
}

impl Profiler for FlameGraphProfiler<'_> {
    fn start_profiling(&mut self, _benchmark_id: &str, _benchmark_dir: &Path) {
        self.active_profiler = Some(ProfilerGuard::new(self.frequency).unwrap());
    }

    fn stop_profiling(&mut self, _benchmark_id: &str, benchmark_dir: &Path) {
        create_dir_all(benchmark_dir).unwrap();
        let path = benchmark_dir.join("flamegraph.svg");
        let file = File::create(path).unwrap();
        if let Some(profiler) = self.active_profiler.take() {
            profiler
                .report()
                .build()
                .unwrap()
                .flamegraph(file)
                .expect("Error writing flamegraph");
        }
    }
}
