use common::log;
use common::uuid::Uuid;
use log::info;
use std::collections::HashSet;
use std::hash::Hash;
use std::iter::Map;
use std::net::TcpListener;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;
use common::log::error;

use common::node::{Connection, SlaveWriter};
use common::threadpool;

pub struct Master {
    master_connection: Connection,
    slave_writers: Arc<HashSet<SlaveWriter>>, // Used to communicate with slave. It is read only.
                                              // node_status: Map<Uuid, Node>
                                              // jobs (Jobs to be performed).
}

impl Master {
    pub fn new(master_connection: Connection, slave_connections: Vec<Connection>) -> Self {
        let mut slave_writers = HashSet::new();
        for slave_connection in slave_connections {
            slave_writers.insert(SlaveWriter::new(slave_connection));
        }

        let slave_writers = Arc::new(slave_writers);
        Self {
            master_connection,
            slave_writers,
        }
    }

    pub fn start(&self) {
        // INit place for all master tasks.
        let threadpool = threadpool::Threadpool::new(10);
        let master_socket = self.master_connection.get_socket(); // Listen to incoming packets using TcpListnere.

        // heartbeat for all slaves.
        let slave_writers = self.slave_writers.clone();
        threadpool.execute(move || heartbeat(slave_writers)); // heartbeat of slaves.
                                                              // threadpool.execute(move || tcp_listener()); // a thread to listen to data received from slave.
    }
}

// Infinite loop to ping slaves.
fn heartbeat(slave_writer: Arc<HashSet<SlaveWriter>>) {
    loop {
        for slave in slave_writer.iter() {
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

        // for slave in  slave_writer {
        // }
        thread::sleep(Duration::new(5, 0));
    }
}
