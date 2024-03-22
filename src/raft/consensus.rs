use std::{cmp::min, collections::HashMap,  time::{Duration, Instant}};

use crate::raft::common::{Pid, LogIndex, Term};
use crate::raft::storage::LogStorage;

use prost::Message;
use rand::Rng;


#[derive(Default, Clone, Debug)]
pub struct State<S: LogStorage> {
    current_term: Term,
    voted_for: Option<Pid>,
    log: S,
    commit_idx: LogIndex,
    last_applied: LogIndex,
}



#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Entry {

    #[prost(uint32, tag="1")]
    term: Term,

    #[prost(bytes, tag="2")]
    data: Vec<u8>
}


impl From<&[u8]> for Entry {
    fn from(value: &[u8]) -> Self {
        Entry::decode(value).unwrap()
    }
}


impl From<Entry> for (Term, Vec<u8>) {
    fn from(value: Entry) -> Self {
        let Entry {term, data} = value;
        (term, data)
    }
}


#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AppendEntries {
    #[prost(uint32, tag="1")]
    term: Term,
    
    #[prost(uint32, tag="2")]
    leader_id: Pid,
    
    #[prost(uint64, tag="3")]
    leader_commit: LogIndex,
      
    #[prost(uint64, tag="4")]
    prev_log_idx: LogIndex,
        
    #[prost(uint32, tag="5")]
    prev_log_term: Term,

    #[prost(message, repeated, tag="6")]
    entries: Vec<Entry>
}


#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RequestVote {
    #[prost(uint32, tag="1")]
    term: Term,
    #[prost(uint32, tag="2")]
    candidate_id: Pid,
    #[prost(uint64, tag="3")]
    last_log_idx: LogIndex,
    #[prost(uint32, tag="4")]
    last_log_term: Term
}


#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AppendEntriesReply {
    #[prost(uint32, tag="1")]
    from: Pid,
    #[prost(uint32, tag="2")]
    term: Term,
    #[prost(bool, tag="3")]
    success: bool
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RequestVoteReply {
    #[prost(uint32, tag="1")]
    from: Pid,
    #[prost(uint32, tag="2")]
    term: Term,
    #[prost(bool, tag="3")]
    vote_granted: bool,
}

#[derive(Default, Clone, Debug, PartialEq)]
pub enum RaftState {
    #[default]
    Follower,
    Candidate,
    Leader
}


#[derive(Clone, PartialEq, ::prost::Message)]
struct LogEntry {
    #[prost(uint32, tag="1")]
    term: Term,

    #[prost(bytes, tag="2")]
    data: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct RaftConsensus<S: LogStorage> {
    pid: Pid,
    election_timeout: Instant,
    raft_state: RaftState,
    peers: Vec<Pid>,
    current_term: Term,
    voted_for: Option<Pid>,
    log: S,
    commit_idx: LogIndex,
    last_applied: LogIndex,
    // When leader
    next_idx: HashMap<Pid, LogIndex>,
    match_idx: HashMap<Pid, LogIndex>,
}





impl<S: LogStorage> RaftConsensus<S> {

    pub fn new(pid: Pid, peers: Vec<Pid>, log: S) -> RaftConsensus<S> {
        let next_idx = peers.iter().map(|peer_pid| (*peer_pid, 1)).collect();
        let match_idx = peers.iter().map(|peer_pid| (*peer_pid, 0)).collect();
        let election_timeout = Duration::from_millis(rand::thread_rng().gen_range(150..300));
        RaftConsensus {
            pid: pid,
            election_timeout: Instant::now().checked_add(election_timeout).unwrap(),
            raft_state: Default::default(),
            peers: peers,
            current_term: Default::default(),
            voted_for: Default::default(),
            log: log,
            commit_idx: Default::default(),
            last_applied: Default::default(),
            next_idx: next_idx,
            match_idx: match_idx,
        }
    }

    pub fn reset_election_timer(&mut self) {
        let election_timeout = Duration::from_millis(rand::thread_rng().gen_range(150..300));
        self.election_timeout = Instant::now().checked_add(election_timeout).unwrap();
    }

    pub fn tick(&mut self) -> Option<RequestVote> {
        let now = Instant::now();
        let elapsed = now.duration_since(self.election_timeout);
        if elapsed == Duration::ZERO {
            // Trigger a new election
            return Some(self.new_election());
        }
        return None;
    }

    pub fn new_election(&mut self) -> RequestVote {
        self.raft_state = RaftState::Candidate;
        self.voted_for = Some(self.pid);
        self.current_term += 1;
        self.reset_election_timer();

        let last_log_term = self.log.get_last::<Entry>()
            .map(|(_, entry)| entry.term)
            .unwrap_or(0);

        RequestVote {
            term: self.current_term,
            candidate_id: self.pid,
            last_log_idx: self.commit_idx,
            last_log_term: last_log_term
        }
    }
    
    pub async fn become_follower(&mut self) {
        self.raft_state = RaftState::Follower;
    }

    fn new_append_failed(&self) -> AppendEntriesReply {
        AppendEntriesReply {
            from: self.pid,
            term: self.current_term,
            success: false
        }
    }

    fn commit(&mut self, leader_commit: LogIndex) {
        if leader_commit > self.commit_idx {
            let last_log_entry = self.log.get_last_log_index().unwrap_or(0);

            let new_commit_idx = min(leader_commit, last_log_entry);
            for i in self.commit_idx..(new_commit_idx+1) {
                self.log.commit_log(i).expect("TODO")
            }
            self.commit_idx = new_commit_idx;
        }
    }

    pub fn handle_append_entries(&mut self, ae: AppendEntries) -> AppendEntriesReply {
        if ae.term > self.current_term {
            self.become_follower();
        }
        
        if self.raft_state == RaftState::Leader {
            return self.new_append_failed();
        }

        if ae.term < self.current_term {
            return self.new_append_failed();
        }
        
        let my_prev_term = self.log
            .get(ae.prev_log_idx)
            .map(|bytes| LogEntry::decode(bytes.as_ref()).unwrap())
            .map(|log_entry| log_entry.term)
            .unwrap_or(0);

        if my_prev_term != ae.prev_log_term {
            return self.new_append_failed()
        }
    
        let start = ae.prev_log_idx + 1;
        let end = (ae.entries.len() as u64) + ae.prev_log_idx;

        if ae.entries.last().is_some() {
            self.log.delete_from((end + 1)..).expect("Failed to delete entries");
        }
        
        let mut serialized_entries: Vec<Vec<u8>> = ae.entries
            .iter()
            .map(|_| Vec::new())
            .collect();

        ae.entries
            .iter()
            .enumerate()
            .for_each(|(i, entry)| entry.encode(&mut serialized_entries[i]).unwrap());

        self.log.insert_slice_at(start, &serialized_entries).unwrap();
        
        self.commit(ae.leader_commit);

        return AppendEntriesReply {
            from: self.pid,
            term: self.current_term,
            success: true
        }
    }
}

