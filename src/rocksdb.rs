use rust_rocksdb::{Direction, IteratorMode, Options, ReadOptions, DB};
mod raft;


struct Persistent {
    db: DB
}


// impl Storage for Persistent {
//     fn initial_state(&self) -> raft::Result<raft::prelude::RaftState> {
//         todo!()
//     }

//     fn entries(
//         &self,
//         low: u64,
//         high: u64,
//         max_size: impl Into<Option<u64>>,
//         context: raft::GetEntriesContext,
//     ) -> raft::Result<Vec<raft::prelude::Entry>> {
//         let from_key = low.to_le_bytes();
//         let to_end = high.to_le_bytes();
//         self.db.iterator_opt(mode, readopts)
//         self.db.iterator(IteratorMode::From((), ()))
//     }

//     fn term(&self, idx: u64) -> raft::Result<u64> {
//         todo!()
//     }

//     fn first_index(&self) -> raft::Result<u64> {
//         todo!()
//     }

//     fn last_index(&self) -> raft::Result<u64> {
//         todo!()
//     }

//     fn snapshot(&self, request_index: u64, to: u64) -> raft::Result<raft::prelude::Snapshot> {
//         todo!()
//     }
// }


fn main() {
    let path = "./storage";
    let db = DB::open_default(path).unwrap();
    db.put(b"my key", b"my value");
    let it = db.iterator(IteratorMode::Start);

    for row in it {
        let (key, value) = row.unwrap();
        let key = String::from_utf8_lossy(&key);
        let value = String::from_utf8_lossy(&value);
        println!("Saw {:?} {:?}", key, value);
    }

    // match db.get(b"my key") {
    //    Ok(Some(value)) => println!("retrieved value {}", String::from_utf8(value).unwrap()),
    //    Ok(None) => println!("value not found"),
    //    Err(e) => println!("operational problem encountered: {}", e),
    // }
    // db.delete(b"my key").unwrap();
}