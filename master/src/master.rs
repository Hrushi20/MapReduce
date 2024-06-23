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
use std::sync::mpsc::{Receiver, Sender};
use std::thread;
use std::time::Duration;
use common::config_structs::MapReduceConfig;
use common::log::error;
use crate::map_job::MapJob;

use common::node::{Connection, NodeHealth, NodeStatus, SlaveWriter};
use common::threadpool;
use crate::map_job;

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

        let threadpool = threadpool::Threadpool::new(10);
        let slave_connections = self.slave_connections.clone();

        let node_status = mpsc::channel();
        threadpool.execute(move || heartbeat(slave_connections.clone(), &node_status.0)); // heartbeat of slaves.
        threadpool.execute(move || update_node_state(&node_status.1));

        // Map Thread.
        // Wait for all map operations.
        // thread::spawn(move || map(&self.map_reduce_config, slave_connections.clone())).join().unwrap();

        // Reduce Thread.
        // Wait for all reduce operations.
        // thread::spawn(move || reduce()).join().unwrap();

        // Clean-ups and result aggregation.
    }
}

fn update_node_state(receiver: &Receiver<NodeHealth>){
    loop {
       let node_health = receiver.recv().unwrap();
        match node_health.state {
            NodeStatus::Alive => info!("Node: {} is Alive", node_health.id),
            NodeStatus::Died => error!("Node: {} has Died", node_health.id),
            _ => error!("Invalid State! Shouldn't be here")
        }
    }
}

// Infinite loop to ping slaves.
fn heartbeat(slave_connections: Arc<Vec<Connection>>, sender: &Sender<NodeHealth>) {
    let mut slave_writers = Vec::new();
    for slave_connection in slave_connections.iter() {
        slave_writers.push(SlaveWriter::new(slave_connection));
    }

    loop {
        for slave in slave_writers.iter_mut() {
            let heartbeat = slave.ping(); // Blocking IO. Can't ping other nodes.
            let node_status = match heartbeat {
                Some(usize) => {
                    NodeStatus::Alive
                } // The node is up and running.
                None => {
                    NodeStatus::Died
                }        // The node is down. Update the state of the node.
            };

            // Send message only if the state has been changed.
            // Avoid unnecessary state transmission messages.
            sender.send(NodeHealth::new(slave.id.clone(), node_status)).unwrap()
        }

        thread::sleep(Duration::new(5, 0));
    }
}

// fn map(map_reduce_config: &MapReduceConfig, slave_connections: Arc<Vec<Connection>>){
//
//     // Connection to Slaves.
//     let mut slave_writers = Vec::new();
//     let mut slave_nodes = HashSet::new();
//     for slave_connection in slave_connections.iter() {
//         slave_writers.push(SlaveWriter::new(slave_connection));
//         slave_nodes.insert(&slave_connection.id);
//     }
//
//     // Map Thread.
//     let map_tasks = construct_map_tasks(map_reduce_config);
//
//     let mut finished_tasks = Arc::new(Mutex::new(Vec::new()));
//     // Pass the above finished_tasks to various threads and store the result in the above array.
//
//     // Waiting for all tasks to finish.
//     loop {
//         if finished_tasks.len() == map_tasks.len() {
//             break;
//         }
//     }
//
//
// }

fn reduce(){
    // Reduce Thread.
}

fn construct_map_tasks(map_reduce_config: &MapReduceConfig) -> VecDeque<MapJob>{
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