use std::borrow::Cow;

use reqwest::multipart::Part;

#[tokio::main]
async fn main() {
    let client = reqwest::Client::new();
    let form = reqwest::multipart::Form::new().part(
        "file",
        Part::bytes(Cow::from("your content".as_bytes())).file_name("yourfile.txt"),
    );
    let response = client
        .post("http://127.0.0.1:8888/api/Example")
        .multipart(form)
        .send()
        .await
        .unwrap();
    println!("{:?}", response);
}
