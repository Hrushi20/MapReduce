use std::collections::HashSet;
use std::hash::Hash;
use std::iter::Map;
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use uuid::Uuid;

use crate::node::{NodeConfig, SlaveWriter};
use crate::threadpool;

pub struct Master {
    master_config: NodeConfig,
    slave_writers: Arc<HashSet<SlaveWriter>>, // Used to communicate with slave. It is read only.
    // node_status: Map<Uuid, Node>
    // jobs (Jobs to be performed).
}

impl Master {
    pub fn new(master_config: NodeConfig, slave_nodes_config: Vec<NodeConfig>) -> Self {

        let mut slave_writers = HashSet::new();
        for slave_config in slave_nodes_config {
            slave_writers.insert(SlaveWriter::new(slave_config));
        };

        let slave_writers = Arc::new(slave_writers);
        Self {
           master_config,
           slave_writers
        }
    }

    pub fn start(&self){
        // INit place for all master tasks.
        let threadpool = threadpool::Threadpool::new(10);
        let master_socket = self.master_config.get_socket(); // Listen to incoming packets using TcpListnere.

        // heartbeat for all slaves.
        let slave_writers = self.slave_writers.clone();
        threadpool.execute(move || heartbeat(slave_writers)); // heartbeat of slaves.
        // threadpool.execute(move || tcp_listener()); // a thread to listen to data received from slave.
    }
}

// Infinite loop to ping slaves.
fn heartbeat(slave_writer: Arc<HashSet<SlaveWriter>>){
    loop {
        for slave in slave_writer.iter() {
            let heartbeat = slave.ping(); // Blocking IO. Can't ping other nodes.
            match heartbeat {
                Some(usize) => {}, // The node is up and running.
                None => {}  // The node is down. Update the state of the node.
            }
        }

        // for slave in  slave_writer {
        // }
        thread::sleep(Duration::new(5, 0));
    }
}