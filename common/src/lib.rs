pub mod node;
pub mod rpc;
pub mod threadpool;
pub mod config_structs;
pub mod json_rpc;
mod utils;
pub use env_logger;
use env_logger::{Builder, Target};

pub use log;
use log::LevelFilter;

pub use uuid;
pub use serde_json;

fn main(){
    println!("Inside common")
}

pub fn init_logger(){
    Builder::new().filter_level(LevelFilter::Trace).init();
}
