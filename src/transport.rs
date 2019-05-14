use std::collections::HashMap;
use std::thread;
use std::boxed::FnBox;

use crossbeam_channel::{unbounded, Receiver, Sender};
use failure::Error;
use raft::eraftpb::{Message as RaftMessage, MessageType};
use serde::{Deserialize, Serialize};
use raft::SnapshotStatus;
use reqwest;
use protobuf::Message;

use crate::util::Row;


// op: read 1, write 2, delete 3.
// op: status 128
#[derive(Serialize, Deserialize, Default)]
pub struct Request {
    pub id: u64,
    pub op: u32,
    pub row: Row,
}

impl Request {
    // to raft log data
    pub fn to_data(&self) -> Result<Vec<u8>, Error> {
        let data = serde_json::to_vec(self)?;
        Ok(data)
    }
    // from raft log data
    pub fn from_data(data: &[u8]) -> Result<Self, Error> {
        let req: Request = serde_json::from_slice(data)?;
        Ok(req)
    }
}

#[derive(Serialize, Deserialize, Default)]
pub struct Response {
    pub id: u64,
    pub ok: bool,
    pub op: u32,
    pub value: Option<Vec<u8>>,
}


pub type RequestCallback = Box<FnBox(Response) + Send>;

pub enum Msg {
    Propose {
        request: Request,
        cb: RequestCallback,
    },
    Raft(RaftMessage),
    ReportUnreachable(u64),
    ReportSnapshot {
        id: u64,
        status: SnapshotStatus,
    },
}



#[derive(Serialize, Deserialize, Default)]
pub struct Status {
    pub leader_id: u64,
    pub id: u64,
    pub first_index: u64,
    pub last_index: u64,
    pub term: u64,
    pub apply_index: u64,
    pub commit_index: u64,
}


pub struct Transport {
    sender: Sender<Msg>,
    node_chs: HashMap<u64, Sender<RaftMessage>>,
}

impl Transport {
    pub fn new(sender: Sender<Msg>) -> Transport {
        Transport {
            sender: sender,
            node_chs: HashMap::new(),
        }
    }

    pub fn start(&mut self, node_addrs: HashMap<u64, String>) {
        for (id, addr) in node_addrs.iter() {
            let (s, r) = unbounded();
            self.node_chs.insert(*id, s);

            let id = *id;
            let addr = addr.clone();
            let sender = self.sender.clone();
            thread::spawn(move || {
                on_transport(r, id, addr, sender);
            });
        }
    }

    pub fn send(&self, id: u64, msg: RaftMessage) {
        if let Some(s) = self.node_chs.get(&id) {
            s.send(msg);
        }
    }
}

fn on_transport(ch: Receiver<RaftMessage>, id: u64, addr: String, sender: Sender<Msg>) {
    let client = reqwest::Client::new();
    let url = format!("http://{}/raft", addr);
    while let Ok(msg) = ch.recv() {
        let value = msg.write_to_bytes().unwrap();
        let is_snapshot = msg.get_msg_type() == MessageType::MsgSnapshot;
        if let Err(_) = client.post(&url).body(value).send() {
            sender.send(Msg::ReportSnapshot {
                id: id,
                status: SnapshotStatus::Failure,
            });
            sender.send(Msg::ReportUnreachable(id));
        }

        if is_snapshot {
            sender.send(Msg::ReportSnapshot {
                id: id,
                status: SnapshotStatus::Finish,
            });
        }
    }
}
