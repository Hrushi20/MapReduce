use std::net::IpAddr;
use std::str::FromStr;
use std::time::Duration;
use serde;
use serde::Deserialize;
use crate::node::Connection;

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct MasterConfig {
    master_config: NodeConfig,
    slave_config: Vec<NodeConfig>,
    pub map_reduce_config: MapReduceConfig
}

impl MasterConfig {
    pub fn get_master_connection(&self) -> Connection {
       Connection::new(&self.master_config.hostname, self.master_config.port, Duration::new(5, 0)) // Default Duration
    }

    pub fn get_slave_connections(&self) -> Vec<Connection>{
        let mut slave_connections = Vec::with_capacity(self.slave_config.len());

        for config in self.slave_config.iter(){
            let connection = Connection::new(&config.hostname, config.port, Duration::new(5, 0)); // Default Duration
            slave_connections.push(connection);
        }

        slave_connections
    }
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct NodeConfig {
    hostname: String,
    port: u16
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct MapReduceConfig {
    pub map_nodes: usize,
    pub reduce_nodes: usize,
    pub input_file: String
}
