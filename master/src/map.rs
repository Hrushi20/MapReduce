use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::thread::sleep;
use log::info;
use common::json_rpc::RpcRequest;
use common::node::{NodeStatus, SlaveWriter};
use common::{init_logger, serde_json};
use common::uuid::Uuid;
use crate::map_task::MapTask;
use crate::node_status::NodeLifeCycle;

pub fn map(map_tasks: &mut VecDeque<MapTask>, node_lifecycle: Arc<NodeLifeCycle>) {
    log::info!("Init Execution of map Tasks.");
    // Mapping of map_task id with node id.
    let mut task_slave_mapping = HashMap::new();

    loop {
        let value = Arc::clone(&node_lifecycle);
        let map_node_id = value.get_idle_node();
        // No idle node is present.
        if map_node_id.is_none() {
           continue;
        }

        log::info!("Idle node: {} found", map_node_id.unwrap());
        // While execute this statement, the above node could have died.
        let map_task = map_tasks.pop_front();

        if map_task.is_none() {
            log::info!("Executed all Map tasks.");
            break;
        }

        // Should be Start of transaction
        let map_task = map_task.unwrap();
        let map_node_id = map_node_id.unwrap();
        task_slave_mapping.insert(map_task.id, map_node_id);

        value.update_state(map_node_id, NodeStatus::Processing);
        log::info!("Executing MapTask: {} on Slave Node: {}", map_task.id, map_node_id);
        thread::spawn(move || execute_map(Arc::new(map_task), value, Arc::new(map_node_id)));
        // Should be End of transaction

        // thread::spawn()
    }

    // Need another thead which keep checking if node fails and
    // reassigns a MapTask to new Node.
}

// Format for Params:
// Map Task Id, Task Start Idx, Task End Idx.
fn execute_map(task: Arc<MapTask>, node_lifecycle: Arc<NodeLifeCycle>, map_node_id: Arc<Uuid>){
    // If any error, update the state of slave

    let params = [String::from(task.id), task.start.to_string(), task.end.to_string() ];
    let rpc_request = RpcRequest::new(String::from("execute_map"), Some(params.to_vec()));
    let connection = node_lifecycle.get_connection(&map_node_id);
    println!("Rpc Request: {:?}", rpc_request);
    let mut slave_writer = SlaveWriter::new(&connection);
    let bytes_written = slave_writer.write_request(&rpc_request);
}