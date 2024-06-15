use std::any::Any;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};
use std::thread;
use log::{error, info};
use common::node::{Connection};

pub fn start(connection: Connection){
    let listener = TcpListener::bind(connection.socket).expect("Unable to bind to Port");
    info!("Slave listening on PORT: {}", connection.get_socket().port());

    // Create a HashMap<String, FnPtr>
    let rpc_funcs = register_rpc_fn();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                info!("Request Received: {}", stream.local_addr().unwrap());
                let rpc_func_clone = rpc_funcs.clone();
                thread::spawn(move || {
                    handle_client(stream, rpc_func_clone)
                });
            }
            Err(e) => {
                error!("Connection failed: {}", e);
            }
        }
    }
}

fn handle_client(mut stream: TcpStream, rpc_funcs: Arc<RwLock<HashMap<String, Box<dyn Any + Send + Sync>>>>) {
    let mut buf_reader = BufReader::new(&mut stream);

    // Keeping the connection open.
    loop {
        let received = buf_reader.fill_buf().unwrap().to_vec();
        buf_reader.consume(received.len());
        String::from_utf8(received)
            .map(|msg| println!("{}", msg))
            .map_err(|_| {
                println!("Couldn't parse received stream");
            }).expect("Can't convert to String");

        info!("Finished execution");
    }
}

fn register_rpc_fn() -> Arc<RwLock<HashMap<String, Box<dyn Any + Send + Sync>>>> {
    let rpc_funcs: Arc<RwLock<HashMap<String, Box<dyn Any + Send + Sync>>>> = Arc::new(RwLock::new(HashMap::new()));
    rpc_funcs
}

