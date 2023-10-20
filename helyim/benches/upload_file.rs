use std::{collections::HashMap, time::Duration};

use criterion::{criterion_group, criterion_main, Criterion};
use reqwest::blocking::{multipart::Form, Client};
use serde_json::Value;

fn get_file_id(client: &Client) -> Result<HashMap<String, Value>, Box<dyn std::error::Error>> {
    let response = client
        .get("http://localhost:9333/dir/assign")
        .send()?
        .json::<HashMap<String, Value>>()?;
    Ok(response)
}

fn upload(client: &Client, map: &HashMap<String, Value>) -> Result<(), Box<dyn std::error::Error>> {
    if let Value::String(fid) = map.get("fid").unwrap() {
        let form = Form::new().file("Cargo.toml", "Cargo.toml")?;
        client
            .post(format!("http://localhost:8080/{}", fid))
            .multipart(form)
            .send()?;
    }
    Ok(())
}

fn criterion_benchmark(c: &mut Criterion) {
    let client = Client::new();
    let params = get_file_id(&client).unwrap();
    c.bench_function("upload", |b| {
        b.iter(|| {
            upload(&client, &params).unwrap();
        })
    });
}

fn short_warmup() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .sample_size(10_0000)
}

criterion_group! {
    name = benches;
    config = short_warmup();
    targets = criterion_benchmark
}
criterion_main!(benches);
