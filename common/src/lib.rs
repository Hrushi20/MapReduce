pub mod node;
pub mod rpc;
pub mod threadpool;
pub mod config_structs;

pub use env_logger;
use env_logger::{Builder, Target};
pub use log;
use log::LevelFilter;
pub use uuid;

fn main(){
    println!("Inside common")
}

pub fn init_logger(){
    Builder::new().filter_level(LevelFilter::Trace).init();
}
