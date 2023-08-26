# Helyim

-----
[seaweedfs](https://github.com/seaweedfs/seaweedfs) implemented in pure Rust.

### Usage

By default, the master node runs on port 9333, and the volume nodes run on port 8080. Let's start one master node, and one volume node on port 8080. Ideally, they should be started from different machines. We'll use localhost as an example.

Helyim uses HTTP REST operations to read, write, and delete. The responses are in JSON or JSONP format.

#### 1. Start Master Server

```shell
> cargo run --bin helyim master
```

#### 2. Start Volume Servers

```shell
> cargo run --bin helyim volume --port 8080 --dir ./vdata:70 --dir ./v1data:10
```

#### 3. Write File

To upload a file: first, send a HTTP POST, PUT, or GET request to `/dir/assign` to get an `fid` and a volume server URL:

```bash
> curl http://localhost:9333/dir/assign
{"fid":"6,16b7578a5","url":"127.0.0.1:8080","public_url":"127.0.0.1:8080","count":1,"error":""}
```

Second, to store the file content, send a HTTP multi-part POST request to `url + '/' + fid` from the response:

```bash
> curl -F file=@./sun.jpg http://127.0.0.1:8080/6,16b7578a5
{"name":"sun.jpg","size":1675569,"error":""}
```

To update, send another POST request with updated file content.

For deletion, send an HTTP DELETE request to the same `url + '/' + fid` URL:

```bash
> curl -X DELETE http://127.0.0.1:8080/6,16b7578a5
```

### Acknowledgments

- [seaweedfs](https://github.com/seaweedfs/seaweedfs) - SeaweedFS is a fast distributed storage system for blobs, objects, files, and data lake, for billions of files! Blob store has O(1) disk seek, cloud tiering. Filer supports Cloud Drive, cross-DC active-active replication, Kubernetes, POSIX FUSE mount, S3 API, S3 Gateway, Hadoop, WebDAV, encryption, Erasure Coding.
- [zergling](https://github.com/july2993/zergling) - seaweedFS re-implemented in Rust.