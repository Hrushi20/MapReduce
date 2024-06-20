use std::env;
use crate::master::Master;
use config;
use config::{Config, ConfigBuilder, ConfigError, File};
use common::config_structs::{MasterConfig};
use common::init_logger;
use common::log::info;

mod master;
mod map_job;

fn main() {
    init_logger();
    info!("Started Program Execution");

    let config = read_config().expect("Can't deserialize config file");

    let master_connection = config.get_master_connection();
    let slave_connection = config.get_slave_connections();

    let master = Master::new(master_connection, slave_connection, config.map_reduce_config);
    master.start();
}

fn read_config() -> Result<MasterConfig, ConfigError> {
    let args: Vec<String> = env::args().collect();
    // let config_file_path = args.get(1).expect("Config File Path Not found").clone();
    let config_file_path = "/Users/pc/my/code/openSource/map-reduce/config/master_config.toml";
    info!("Reading config: {}", config_file_path);
    let config = Config::builder()
            .add_source(File::with_name(&config_file_path))
            .build().expect("Couldn't parse config file");

    config.try_deserialize()
}
