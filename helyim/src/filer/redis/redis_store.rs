use faststr::FastStr;
use redis::{Client, Commands, Connection, SetExpiry, SetOptions};

use crate::{
    filer::{entry::Entry, FilerError, FilerStore},
    util::file::{dir_and_name, new_full_path},
};

const DIR_LIST_MARKER: &str = "\x00";

struct RedisStore {
    addr: FastStr,
    client: Option<Client>,
}

impl RedisStore {
    pub fn new(addr: &str) -> Self {
        Self {
            addr: addr.to_owned().into(),
            client: None,
        }
    }

    fn get_connection(&self) -> Result<Connection, FilerError> {
        if let Some(client) = &self.client {
            let con = client.get_connection().map_err(|err| {
                FilerError::FileStoreErr(FastStr::new(format!("connect redis err, {}", err)))
            })?;
            Ok(con)
        } else {
            Err(FilerError::FileStoreErr("get connection error".into()))
        }
    }
}

impl FilerStore for RedisStore {
    fn name(&self) -> &str {
        "redis"
    }

    fn initialize(&mut self) -> Result<(), FilerError> {
        let client = redis::Client::open(self.addr.to_string()).map_err(|err| {
            FilerError::InitErr(FastStr::new(format!("create redis client err, {}", err)))
        })?;

        self.client = Some(client);
        Ok(())
    }

    fn insert_entry(&self, entry: &Entry) -> Result<(), FilerError> {
        let value = entry.encode_attributes_and_chunks()?;
        let mut con = self.get_connection()?;
        let opt =
            SetOptions::default().with_expiration(SetExpiry::EX(entry.ttl().try_into().unwrap()));
        con.set_options(entry.path(), value, opt).map_err(|err| {
            FilerError::FileStoreErr(
                format!("set to redis err, key: {}, err: {}", entry.path(), err).into(),
            )
        })?;

        let (dir, name) = dir_and_name(&entry.path);

        if !name.is_empty() {
            con.sadd(gen_directory_list_key(dir), name).map_err(|err| {
                FilerError::FileStoreErr(format!("sadd to redis err: {}", err).into())
            })?;
        }

        Ok(())
    }

    fn update_entry(&self, entry: &Entry) -> Result<(), FilerError> {
        self.insert_entry(entry)
    }

    fn find_entry(&self, path: &str) -> Result<Option<Entry>, FilerError> {
        let mut con = self.get_connection()?;
        let value: Vec<u8> = con.get(path).map_err(|err| {
            FilerError::FileStoreErr(
                format!("get from redis err, key: {}, err: {}", path, err).into(),
            )
        })?;

        if value.is_empty() {
            return Ok(None);
        }

        let mut entry = Entry {
            path: path.to_owned().clone().into(),
            ..Default::default()
        };
        entry.decode_attributes_and_chunks(value)?;
        Ok(Some(entry))
    }

    fn delete_entry(&self, path: &str) -> Result<(), FilerError> {
        let mut con = self.get_connection()?;
        con.del(path).map_err(|err| {
            FilerError::FileStoreErr(
                format!("del redis key err, key: {}, err: {}", path, err).into(),
            )
        })?;

        let (dir, name) = dir_and_name(path);

        if !name.is_empty() {
            con.srem(gen_directory_list_key(dir), name).map_err(|err| {
                FilerError::FileStoreErr(format!("srem to redis err: {}", err).into())
            })?;
        }

        Ok(())
    }

    fn list_directory_entries(
        &self,
        dir_path: &str,
        start_filename: &str,
        include_start_file: bool,
        limit: usize,
    ) -> Result<Vec<Entry>, FilerError> {
        let mut con = self.get_connection()?;
        let mut members: Vec<String> = con
            .smembers(gen_directory_list_key(dir_path.into()))
            .map_err(|err| {
                FilerError::FileStoreErr(format!("redis smembers err: {}", err).into())
            })?;

        // list filename
        if !start_filename.is_empty() {
            let mut t = Vec::new();
            let start = start_filename.to_string();
            members.into_iter().for_each(|val| {
                if include_start_file {
                    if val.cmp(&start).is_ge() {
                        t.push(val);
                    }
                } else if val.cmp(&start).is_gt() {
                    t.push(val);
                }
            });
            members = t;
        }

        // sort
        members.sort();

        // limit
        members = members[..limit].to_vec();

        let mut entries = Vec::new();
        members.into_iter().for_each(|filename| {
            let path = new_full_path(dir_path, filename.as_str());
            if let Ok(Some(entry)) = self.find_entry(&path) {
                entries.push(entry)
            }
        });

        Ok(entries)
    }

    fn begin_transaction(&self) -> Result<(), FilerError> {
        todo!()
    }

    fn commit_transaction(&self) -> Result<(), FilerError> {
        todo!()
    }

    fn rollback_transaction(&self) -> Result<(), FilerError> {
        todo!()
    }
}

fn gen_directory_list_key(dir: String) -> String {
    dir + DIR_LIST_MARKER
}

#[cfg(test)]
mod test {
    use std::time::SystemTime;

    use faststr::FastStr;
    use redis::{cmd, Commands, RedisError};

    use super::RedisStore;
    use crate::filer::{
        entry::{Attr, Entry},
        FilerError, FilerStore,
    };

    #[test]
    fn fetch_an_integer() -> Result<(), RedisError> {
        // connect to redis
        let client = redis::Client::open("redis://127.0.0.1/")?;
        let mut con = client.get_connection()?;
        // throw away the result, just make sure it does not fail
        con.set("my_key", 42)?;
        // read back the key and return it.  Because the return value
        // from the function is a result for integer this will automatically
        // convert into one.
        let val: i32 = con.get("my_key")?;

        assert_eq!(val, 42);

        Ok(())
    }

    #[test]
    fn test_not_exist_key() -> Result<(), RedisError> {
        // connect to redis
        let client = redis::Client::open("redis://127.0.0.1/")?;
        let mut con = client.get_connection()?;
        con.del("my_key")?;
        let result: Result<Vec<u8>, RedisError> = cmd("GET").arg("my_key").query(&mut con);

        println!("{:?}", result);
        Ok(())
    }

    #[test]
    fn test_insert() -> Result<(), FilerError> {
        let mut filer_store = RedisStore::new("redis://127.0.0.1/");
        filer_store.initialize()?;

        let path: FastStr = "/etc/ceph/ceph.conf".into();
        let entry = Entry {
            path: path.clone(),
            attr: Attr {
                mtime: SystemTime::now(),
                crtime: SystemTime::now(),
                mode: 0644,
                uid: 1,
                gid: 1,
                mime: "application/zip".into(),
                replication: "r".into(),
                collection: "c".into(),
                ttl: 30,
            },
            chunks: vec![],
        };

        filer_store.insert_entry(&entry)?;

        let result = filer_store.find_entry(&path)?;

        if let Some(entry_n) = result {
            println!("{:?}", entry_n);
        }

        Ok(())
    }
}
