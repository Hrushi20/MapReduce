use std::collections::{HashMap, HashSet};
use std::sync::RwLock;
use uuid::Uuid;
use common::node::{Connection, NodeStatus};

pub struct NodeLifeCycle {
    // This can be a HashSet of UUID.
    // Connections can be another data structure not tied to NodeLifeCycle.
    // There is no use of Adding a Connection here.
    pub idle: RwLock<HashSet<Uuid>>,
    pub dead: RwLock<HashSet<Uuid>>,
    pub processing: RwLock<HashSet<Uuid>>,
    pub connections: HashMap<Uuid, Connection>
}

impl NodeLifeCycle {
   pub fn init(slave_connections: Vec<Connection>) -> Self {
       let mut dead = HashSet::new();
       let mut connections = HashMap::new();
       for slave_connection in slave_connections {
           dead.insert(slave_connection.id);
           connections.insert(slave_connection.id, slave_connection);
       }

       Self {
           dead: RwLock::new(dead),
           idle: RwLock::new(HashSet::new()),
           processing: RwLock::new(HashSet::new()),
           connections
       }
   }

    pub fn is_dead(&self, id: &Uuid) -> bool {
        self.dead.read().unwrap().contains(id)
    }

    pub fn is_idle(&self, id: &Uuid) -> bool {
        self.idle.read().unwrap().contains(id)
    }

    pub fn is_processing(&self, id: &Uuid) -> bool {
        self.processing.read().unwrap().contains(id)
    }

    pub fn get_idle_node(&self) -> Option<Uuid> {
       let mut idle_node = self.idle.read().unwrap();
        match idle_node.iter().next() {
            Some(node) => Some(node.clone()),
            None => None
        }
    }

    // Any way to simply avoiding cloned data.
    pub fn get_connection(&self, id: &Uuid) -> Connection {
       self.connections.get(id).unwrap().clone()
    }

    pub fn update_state(&self, node_id: Uuid, node_state: NodeStatus){
        match node_state {
            NodeStatus::Idle => {
                if self.is_dead(&node_id) {
                   self.idle.write().unwrap().insert(self.dead.write().unwrap().take(&node_id).unwrap());
                }else {
                    self.idle.write().unwrap().insert(self.processing.write().unwrap().take(&node_id).unwrap());
                }
            },
            NodeStatus::Processing => {
                if self.is_dead(&node_id) {
                    self.processing.write().unwrap().insert(self.dead.write().unwrap().take(&node_id).unwrap());
                }else {
                    self.processing.write().unwrap().insert(self.idle.write().unwrap().take(&node_id).unwrap());
                }
            },
            NodeStatus::Dead => {
                if self.is_idle(&node_id) {
                    self.dead.write().unwrap().insert(self.idle.write().unwrap().take(&node_id).unwrap());
                }else {
                    self.dead.write().unwrap().insert(self.processing.write().unwrap().take(&node_id).unwrap());
                }
            }
        }
    }
}