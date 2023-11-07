use std::{collections::HashMap, time::Duration};

use bytes::Bytes;
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

fn extract_value<'a>(params: &'a HashMap<String, Value>, key: &str) -> &'a str {
    match params.get(key) {
        Some(Value::String(fid)) => fid,
        _ => panic!("{key} is not found"),
    }
}

fn read_file(client: &Client, url: &str, fid: &str) -> Result<Bytes, Box<dyn std::error::Error>> {
    let response = client.get(format!("http://{url}/{fid}")).send()?.bytes()?;
    assert!(!response.is_empty());
    Ok(response)
}

fn upload(client: &Client, url: &str, fid: &str) -> Result<(), Box<dyn std::error::Error>> {
    let form = Form::new().file("Cargo.toml", "Cargo.toml")?;
    client
        .post(format!("http://{url}/{fid}"))
        .multipart(form)
        .send()?;
    Ok(())
}

fn criterion_benchmark(c: &mut Criterion) {
    let client = Client::new();
    let params = get_file_id(&client).unwrap();
    let fid = extract_value(&params, "fid");
    let url = extract_value(&params, "url");
    upload(&client, url, fid).unwrap();
    c.bench_function("read files", |b| {
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
