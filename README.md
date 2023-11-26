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

### Benchmark

My laptop results on Lenovo IdeaPad Pro 16 (2023) with SSD, CPU: 14 Intel Core i9 5.4GHz.

It seems to be slower than `seaweedfs`, especially in terms of reading.

```console
➜ ./weed benchmark -server=localhost:9333
This is SeaweedFS version 0.76 linux amd64

------------ Writing Benchmark ----------
Completed 15199 of 1048576 requests, 1.4% 15198.1/s 15.3MB/s
Completed 31887 of 1048576 requests, 3.0% 16687.9/s 16.8MB/s
Completed 48439 of 1048576 requests, 4.6% 16551.6/s 16.7MB/s
...
Completed 994044 of 1048576 requests, 94.8% 16645.2/s 16.8MB/s
Completed 1010800 of 1048576 requests, 96.4% 16755.8/s 16.9MB/s
Completed 1027412 of 1048576 requests, 98.0% 16612.2/s 16.7MB/s
Completed 1044319 of 1048576 requests, 99.6% 16907.0/s 17.0MB/s

Concurrency Level:      16
Time taken for tests:   63.249 seconds
Complete requests:      1048576
Failed requests:        0
Total transferred:      1106759553 bytes
Requests per second:    16578.50 [#/sec]
Transfer rate:          17088.29 [Kbytes/sec]

Connection Times (ms)
              min      avg        max      std
Total:        0.1      0.9       29.8      0.4

Percentage of the requests served within a certain time (ms)
   50%      0.9 ms
   66%      1.0 ms
   75%      1.1 ms
   90%      1.3 ms
   95%      1.5 ms
   98%      1.7 ms
   99%      1.8 ms
  100%     29.8 ms

------------ Randomly Reading Benchmark ----------
Completed 89963 of 1048576 requests, 8.6% 89957.6/s 90.5MB/s
Completed 187560 of 1048576 requests, 17.9% 97597.1/s 98.2MB/s
Completed 283486 of 1048576 requests, 27.0% 95925.8/s 96.6MB/s
Completed 382035 of 1048576 requests, 36.4% 98549.4/s 99.2MB/s
Completed 480649 of 1048576 requests, 45.8% 98613.9/s 99.3MB/s
Completed 583585 of 1048576 requests, 55.7% 102933.7/s 103.6MB/s
Completed 683954 of 1048576 requests, 65.2% 100370.9/s 101.0MB/s
Completed 782522 of 1048576 requests, 74.6% 98567.9/s 99.2MB/s
Completed 883504 of 1048576 requests, 84.3% 100982.7/s 101.7MB/s
Completed 987320 of 1048576 requests, 94.2% 103814.3/s 104.5MB/s

Concurrency Level:      16
Time taken for tests:   10.600 seconds
Complete requests:      1048576
Failed requests:        0
Total transferred:      1106777459 bytes
Requests per second:    98925.73 [#/sec]
Transfer rate:          101969.36 [Kbytes/sec]

Connection Times (ms)
              min      avg        max      std
Total:        0.0      0.1       2.3      0.1

Percentage of the requests served within a certain time (ms)
   50%      0.1 ms
   95%      0.2 ms
   98%      0.4 ms
  100%      2.3 ms
```

### Acknowledgments

- [seaweedfs](https://github.com/seaweedfs/seaweedfs) - SeaweedFS is a fast distributed storage system for blobs, objects, files, and data lake, for billions of files! Blob store has O(1) disk seek, cloud tiering. Filer supports Cloud Drive, cross-DC active-active replication, Kubernetes, POSIX FUSE mount, S3 API, S3 Gateway, Hadoop, WebDAV, encryption, Erasure Coding.
- [zergling](https://github.com/july2993/zergling) - seaweedFS re-implemented in Rust.