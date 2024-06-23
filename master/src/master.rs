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
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::sync::mpsc::{Receiver, Sender};
use std::thread;
use std::time::Duration;
use common::config_structs::MapReduceConfig;
use common::log::error;
use crate::map_task::MapTask;

use common::node::{Connection, NodeHealth, NodeStatus, SlaveWriter};
use common::threadpool;
use crate::map_task;

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
        let mut slave_status = HashMap::new();

        for slave_connection in slave_connections.iter() {
            slave_status.insert(slave_connection.id.clone(), NodeStatus::Dead);
        }
        let slave_status = Arc::new(RwLock::new(slave_status));

        let node_state_channel = mpsc::channel();
        // let slave_status = slave_status.clone();
        let slave_status_1 = Arc::clone(&slave_status);
        threadpool.execute(move || heartbeat(slave_connections.clone(), &node_state_channel.0, slave_status_1)); // heartbeat of slaves.
        threadpool.execute(move || update_node_state(&node_state_channel.1, Arc::clone(&slave_status)));

        // Map Thread.
        // Wait for all map operations.
        // thread::spawn(move || map(&self.map_reduce_config, slave_connections.clone())).join().unwrap();

        // Reduce Thread.
        // Wait for all reduce operations.
        // thread::spawn(move || reduce()).join().unwrap();

        // Clean-ups and result aggregation.
    }
}

fn update_node_state(receiver: &Receiver<NodeHealth>, mut slave_status: Arc<RwLock<HashMap<Uuid, NodeStatus>>>){
    loop {
       let node_health = receiver.recv().unwrap();
        match node_health.state {
            NodeStatus::Idle => {
                info!("Node: {} is Alive", node_health.id);
                slave_status.write().unwrap().insert(node_health.id, NodeStatus::Idle);
            },
            NodeStatus::Dead => {
                error!("Node: {} has Died", node_health.id);
                slave_status.write().unwrap().insert(node_health.id, NodeStatus::Dead);
            },
            NodeStatus::Processing => {
                info!("Node: {} is Processing", node_health.id);
                slave_status.write().unwrap().insert(node_health.id, NodeStatus::Processing);
            },
            _ => error!("Invalid State! Shouldn't be here")
        }
    }
}

// Infinite loop to ping slaves.
fn heartbeat(slave_connections: Arc<Vec<Connection>>, sender: &Sender<NodeHealth>, slave_status: Arc<RwLock<HashMap<Uuid, NodeStatus>>>) {
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

            if let Some( status) = slave_status.read().unwrap().get(&slave.id) {
                if (node_status == NodeStatus::Dead && *status != node_status)
                || (node_status == NodeStatus::Idle && *status == NodeStatus::Dead) {
                    sender.send(NodeHealth::new(slave.id.clone(), node_status)).unwrap();
                }
            }
        }

        thread::sleep(Duration::new(5, 0));
    }
}

fn map_reduce(map_reduce_config: &MapReduceConfig, slave_connections: Arc<Vec<Connection>>, slave_status: Arc<RwLock<HashMap<Uuid, NodeStatus>>>, sender: &Sender<NodeHealth>){

    // Connection to Slaves.
    let mut slave_writers = HashSet::new();
    for slave_connection in slave_connections.iter() {
        slave_writers.insert(SlaveWriter::new(slave_connection));
    }

    // Map Thread.
    let mut map_tasks = construct_map_tasks(map_reduce_config);
    let actual_map_task = map_tasks.len();

    // let mut finished_tasks = Arc::new(Mutex::new(Vec::new()));
    // Pass the above finished_tasks to various threads and store the result in the above array.
    let mut processed_map_task = 0;

    // let processed_map_task = 0usize;
    while !map_tasks.is_empty() && processed_map_task != actual_map_task {

        if let Some(map_task) = map_tasks.pop_front() {
            // Iterate through slaves and check if any slave is Idle state.
            let mut assigned_slave_id = None;
            for (slave_id, node_status) in slave_status.read().unwrap().iter() {
                if *node_status == NodeStatus::Idle {
                    // Take this node.
                    assigned_slave_id = Some(slave_id.clone());
                }
            }

            // There is no free slave node.
            if assigned_slave_id.is_none() {
                map_tasks.push_front(map_task);
                continue;
            }

            // create a new thread and perform map task
            // thread::spawn(||);

            // Assign the slave to task, update the state to Processing.
            sender.send(NodeHealth::new(assigned_slave_id.unwrap(), NodeStatus::Processing)).unwrap();

        }
    }


}

fn execute_map_task(){

}

fn construct_map_tasks(map_reduce_config: &MapReduceConfig) -> VecDeque<MapTask>{
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

        map_queue.push_back(MapTask::new(start, end)); // End byte not counted. Till End - 1
        start = end;
    }

    info!("Added MapJobs to MapQueue");
    map_queue
}