use common::log;
use common::uuid::Uuid;
use log::info;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::hash::Hash;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::iter::Map;
use std::net::TcpListener;
use std::pin::pin;
use std::rc::Rc;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;
use common::config_structs::MapReduceConfig;
use common::log::error;
use crate::map_job::MapJob;

use common::node::{Connection, SlaveWriter};
use common::threadpool;

pub struct Master {
    master_connection: Connection,
    slave_connections: Arc<Vec<Connection>>,
    map_reduce_config: MapReduceConfig
}

impl Master {
    pub fn new(master_connection: Connection, slave_connections: Vec<Connection>, map_reduce_config: MapReduceConfig) -> Self {
        Self {
            master_connection,
            slave_connections: Arc::new(slave_connections),
            map_reduce_config
        }
    }

    pub fn start(&self) {
        // Init place for all master tasks.

        let map_jobs = construct_map_jobs(&self.map_reduce_config);
        // construct_map_job
        let threadpool = threadpool::Threadpool::new(10);
        let slave_connections = self.slave_connections.clone();
        threadpool.execute(move || heartbeat(slave_connections.clone())); // heartbeat of slaves.
    }
}

// Infinite loop to ping slaves.
fn heartbeat(slave_connections: Arc<Vec<Connection>>) {
    let mut slave_writers = Vec::new();
    for slave_connection in slave_connections.iter() {
        slave_writers.push(SlaveWriter::new(slave_connection));
    }

    loop {
        for slave in slave_writers.iter() {
            let heartbeat = slave.ping(); // Blocking IO. Can't ping other nodes.
            match heartbeat {
                Some(usize) => {
                    info!("Node is up and running");
                } // The node is up and running.
                None => {
                   error!("Node is down");
                }        // The node is down. Update the state of the node.
            }
        }

        thread::sleep(Duration::new(5, 0));
    }
}

fn map_reduce(map_reduce_writers: Arc<HashSet<SlaveWriter>>){

}

fn construct_map_jobs(map_reduce_config: &MapReduceConfig) -> VecDeque<MapJob>{
    let mut map_queue = VecDeque::new();
    let input_file = &map_reduce_config.input_file;
    let map_nodes = &map_reduce_config.map_nodes;
    let mut input_file = File::open(input_file).unwrap();
    let mut file_length = input_file.metadata().unwrap().len() as usize;
    let mut buf_reader = BufReader::new(input_file);
    let segment_size = file_length / *map_nodes;
    let mut start = 0;

    for ctr in 0..*map_nodes{
        let mut overflow = 0;
        let mut end = start + segment_size;
        // last_segment
        if ctr == *map_nodes - 1 {
            end = file_length;
        }else {
            buf_reader.seek(SeekFrom::Start(segment_size as u64)).expect("TODO: panic message");
            let mut vec = Vec::new();
            let overflow = buf_reader.read_until(b' ',  &mut vec).expect("TODO!");
            end += overflow;
        }

        map_queue.push_back(MapJob::new(start, end)); // End byte not counted. Till End - 1
        start = end;
    }

    info!("Added MapJobs to MapQueue");
    map_queue
}