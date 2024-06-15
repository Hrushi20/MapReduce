use std::any::Any;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};
use std::thread;
use log::{error, info};
use common::json_rpc::RpcRequest;
use common::node::{Connection};
use common::serde_json;

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
        let json = String::from_utf8(received).unwrap();
        let rpc_request = RpcRequest::from_json(json);
        let hm = rpc_funcs.read().unwrap();
        let f = hm.get("hello_world").unwrap();
        let f = f.downcast_ref::<fn()>().unwrap();
        f();

        info!("Processed");
    }
}

fn register_rpc_fn() -> Arc<RwLock<HashMap<String, Box<dyn Any + Send + Sync>>>> {
    let mut hm:HashMap<String, Box<dyn Any + Send + Sync>> = HashMap::new();
    hm.insert(String::from("hello_world"), Box::new(hello_world as fn()));

    let rpc_funcs: Arc<RwLock<HashMap<String, Box<dyn Any + Send + Sync>>>> = Arc::new(RwLock::new(hm));
    rpc_funcs
}

fn hello_world(){
    println!("Logging Hello_world");
}
