use std::collections::{HashMap, HashSet};
use std::sync::RwLock;
use uuid::Uuid;
use common::node::{Connection, NodeStatus};

pub struct NodeLifeCycle {
    pub idle: RwLock<HashMap<Uuid,Connection>>,
    pub dead: RwLock<HashMap<Uuid,Connection>>,
    pub processing: RwLock<HashMap<Uuid,Connection>>
}

impl NodeLifeCycle {
   pub fn init(slave_connections: Vec<Connection>) -> Self {
       let mut dead = HashMap::new();
       for slave_connection in slave_connections {
           dead.insert(slave_connection.id, slave_connection);
       }

       Self {
           dead: RwLock::new(dead),
           idle: RwLock::new(HashMap::new()),
           processing: RwLock::new(HashMap::new())
       }
   }

    pub fn is_dead(&self, id: &Uuid) -> bool {
        self.dead.read().unwrap().contains_key(id)
    }

    pub fn is_idle(&self, id: &Uuid) -> bool {
        self.idle.read().unwrap().contains_key(id)
    }

    pub fn is_processing(&self, id: &Uuid) -> bool {
        self.processing.read().unwrap().contains_key(id)
    }

    pub fn update_state(&self, node_id: Uuid, node_state: NodeStatus){
        match node_state {
            NodeStatus::Idle => {
                if self.is_dead(&node_id) {
                   self.idle.write().unwrap().insert(node_id.clone(),self.dead.write().unwrap().remove(&node_id).unwrap());
                }else {
                    self.idle.write().unwrap().insert(node_id.clone(),self.processing.write().unwrap().remove(&node_id).unwrap());
                }
            },
            NodeStatus::Processing => {
                if self.is_dead(&node_id) {
                    self.processing.write().unwrap().insert(node_id.clone(),self.dead.write().unwrap().remove(&node_id).unwrap());
                }else {
                    self.processing.write().unwrap().insert(node_id.clone(),self.idle.write().unwrap().remove(&node_id).unwrap());
                }
            },
            NodeStatus::Dead => {
                if self.is_idle(&node_id) {
                    self.dead.write().unwrap().insert(node_id.clone(),self.idle.write().unwrap().remove(&node_id).unwrap());
                }else {
                    self.dead.write().unwrap().insert(node_id.clone(),self.processing.write().unwrap().remove(&node_id).unwrap());
                }
            }
        }
    }

    fn update(){

    }
}