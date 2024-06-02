use std::time::Duration;
use map_reduce::{master, threadpool};
use map_reduce::node::NodeConfig;


fn main() {
    let master_config = NodeConfig::new("127.0.0.1",8080, Duration::new(5, 0));
    let slave_config = vec![NodeConfig::new(&String::from("127.0.0.1"),8081, Duration::new(5, 0))];
    let master = master::Master::new(master_config, slave_config);

    master.start();
}