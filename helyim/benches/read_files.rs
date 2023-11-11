use std::{collections::HashMap, time::Duration};

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use reqwest::blocking::{multipart::Form, Client};
use serde_json::Value;

fn get_file_id(client: &Client) -> Result<HashMap<String, Value>, Box<dyn std::error::Error>> {
    let response = client
        .get("http://localhost:9333/dir/assign")
        .send()?
        .json::<HashMap<String, Value>>()?;
    Ok(response)
}

fn extract_str_value<'a>(params: &'a HashMap<String, Value>, key: &str) -> &'a str {
    match params.get(key) {
        Some(Value::String(fid)) => fid,
        _ => panic!("{key} is not found"),
    }
}

fn extract_int_value<'a>(params: &'a HashMap<String, Value>, key: &str) -> i64 {
    match params.get(key) {
        Some(Value::Number(num)) => num.as_i64().unwrap(),
        _ => panic!("{key} is not found"),
    }
}

fn read_file(client: &Client, url: &str, fid: &str) -> Result<Bytes, Box<dyn std::error::Error>> {
    let response = client.get(format!("http://{url}/{fid}")).send()?.bytes()?;
    assert!(!response.is_empty());
    Ok(response)
}

fn upload(client: &Client, url: &str, fid: &str) -> Result<i64, Box<dyn std::error::Error>> {
    let form = Form::new().file("Cargo.toml", "Cargo.toml")?;
    let upload = client
        .post(format!("http://{url}/{fid}"))
        .multipart(form)
        .send()?
        .json::<HashMap<String, Value>>()?;
    let size = extract_int_value(&upload, "size");
    Ok(size)
}

fn criterion_benchmark(c: &mut Criterion) {
    let client = Client::new();
    let params = get_file_id(&client).unwrap();
    let fid = extract_str_value(&params, "fid");
    let url = extract_str_value(&params, "url");
    let size = upload(&client, url, fid).unwrap();

    let mut group = c.benchmark_group("read-files-bench");
    group.throughput(Throughput::Bytes(size as u64));
    group.bench_function("read files", |b| {
        b.iter(|| {
            read_file(&client, url, fid).unwrap();
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
