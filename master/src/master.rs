use common::log;
use log::info;
use std::collections::{VecDeque};
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::iter::Map;
use std::net::TcpListener;
use std::pin::pin;
use std::rc::Rc;
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::sync::mpsc::{Receiver, Sender};
use std::thread;
use std::time::Duration;
use common::config_structs::MapReduceConfig;
use common::log::error;
use crate::map_task::MapTask;

use common::node::{Connection, NodeHealth, NodeStatus, SlaveWriter};
use common::threadpool;
use crate::map::map;
use crate::map_task;
use crate::node_status::NodeLifeCycle;

pub struct Master {
    master_connection: Connection,
    slave_connections: Vec<Connection>,
    map_reduce_config: MapReduceConfig
}

impl Master {
    pub fn new(master_connection: Connection, slave_connections: Vec<Connection>, map_reduce_config: MapReduceConfig) -> Self {
        Self {
            master_connection,
            slave_connections,
            map_reduce_config
        }
    }

    pub fn start(&self) {
        // Init place for all master tasks.

        let threadpool = threadpool::Threadpool::new(10);

        let node_lifecycle = Arc::new(NodeLifeCycle::init(self.slave_connections.clone()));

        // Initialize nodes with Unique Id.
        // let slave_status = Arc::new(RwLock::new(node_lifecycle));

        let node_state_channel = mpsc::channel();

        let slave_connections = self.slave_connections.clone();
        let node_lifecycle_2 = Arc::clone(&node_lifecycle);
        let node_lifecycle_3 = Arc::clone(&node_lifecycle_2);
        threadpool.execute(move || heartbeat(node_lifecycle.clone(), &node_state_channel.0, Arc::new(slave_connections))); // heartbeat of slaves.
        threadpool.execute(move || update_node_state(&node_state_channel.1, node_lifecycle_2));

        let mut map_tasks = construct_map_tasks(&self.map_reduce_config);

        map(&mut map_tasks, Arc::clone(&node_lifecycle_3));

        // reduce();
        // Map Thread.
        // Wait for all map operations.
        // thread::spawn(move || map(&self.map_reduce_config, slave_connections.clone())).join().unwrap();

        // Reduce Thread.
        // Wait for all reduce operations.
        // thread::spawn(move || reduce()).join().unwrap();

        // Clean-ups and result aggregation.
    }
}

// Infinite loop.
fn update_node_state(receiver: &Receiver<NodeHealth>, mut slave_status: Arc<NodeLifeCycle>){
    loop {
       let node_health = receiver.recv().unwrap();
        log::info!("Node: {} state update to: {:?} ", node_health.id, node_health.state);
        slave_status.update_state(node_health.id, node_health.state);
    }
}

// Infinite loop to ping slaves.
fn heartbeat(slave_lifecycle: Arc<NodeLifeCycle>, sender: &Sender<NodeHealth>, slave_connections: Arc<Vec<Connection>>) {
    let mut slave_writers = Vec::new();
    for slave_connection in slave_connections.iter() {
        slave_writers.push(SlaveWriter::new(slave_connection));
    }

    loop {
        for slave in slave_writers.iter_mut() {
            let heartbeat = slave.ping(); // Blocking IO. Can't ping other nodes.
            let node_status = match heartbeat {
                Some(usize) => {
                    NodeStatus::Idle // The node is up and running.
                }
                None => {
                    NodeStatus::Dead // The node is down. Update the state of the node.
                }
            };

            if (node_status == NodeStatus::Dead && !slave_lifecycle.is_dead(&slave.id))
            || (node_status == NodeStatus::Idle && !slave_lifecycle.is_idle(&slave.id) && slave_lifecycle.is_dead(&slave.id)) {
                sender.send(NodeHealth::new(slave.id.clone(), node_status)).unwrap();
            }
        }

        thread::sleep(Duration::new(5, 0));
    }
}

fn construct_map_tasks(map_reduce_config: &MapReduceConfig) -> VecDeque<MapTask>{
    let segment_size = 67_108_864usize;  // 64MB
    let mut map_queue = VecDeque::new();
    let input_file = &map_reduce_config.input_file;
    let mut input_file = File::open(input_file).unwrap();
    let mut file_length = input_file.metadata().unwrap().len() as usize; // File size in bytes
    log::info!("Reading File: {:?} of size: {}", &map_reduce_config.input_file, file_length);
    let mut buf_reader = BufReader::new(input_file);
    let no_of_segments = file_length / segment_size;
    let mut start = 0;

    // Split the input file into 64MB size.
    for ctr in 0..no_of_segments {
        let mut overflow = 0;
        let mut end = start;
        // last_segment
        if ctr == no_of_segments - 1 {
            end = file_length;
        }else {
            buf_reader.seek(SeekFrom::Start(segment_size as u64)).expect("TODO: panic message");
            let mut vec = Vec::new(); // Can't know the exact size.
            let overflow = buf_reader.read_until(b' ',  &mut vec).expect("TODO!");
            end += overflow + segment_size;
        }

        map_queue.push_back(MapTask::new(start, end)); // End byte not counted. Till End - 1
        start = end;
    }

    info!("Added {} MapJobs to MapQueue", map_queue.len());
    map_queue
}