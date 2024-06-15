use common::node::{Connection};
use std::env;
use std::str::FromStr;
use std::time::Duration;
use common::log::{error, info, LevelFilter};
use common::{env_logger, init_logger};
use common::env_logger::{Builder, Target};
use common::json_rpc::{RpcRequest, RpcResponse};

mod slave;

fn main() {
    init_logger();

    info!("Started Program Execution");
    let args: Vec<String> = env::args().collect();
    let port = u16::from_str(&args.get(1).expect("No port found").clone()).expect("Couldn't convert String to u16");
    let ip_address = args.get(2);
    let ip_address = match ip_address {
        Some(add) => add.clone(),
        None => String::from("127.0.0.1")
    };

    let connection = Connection::new(&ip_address, port, Duration::new(5,0));
    slave::start(connection);
}
