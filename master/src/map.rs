use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::thread;
use common::node::NodeStatus;
use common::uuid::Uuid;
use crate::map_task::MapTask;

pub fn map(map_task: &VecDeque<MapTask>, slave_status: Arc<RwLock<HashMap<Uuid, NodeStatus>>>){
    // Create a new Thread to execute the map tasks.
    // Mapping of map_task id with node id.
    // let map_assigned_node = Arc::new(HashMap::new());

    // for task in map_task {
    //     thread::spawn(execute_map(Arc::new(task)));
    // }

    // thread::spawn(move||);

    // Need another thead which keep checking if node fails and
    // reassigns a MapTask to new Node.
}

fn execute_map(task: Arc<&MapTask>){
    // If any error, update the state of slave
}