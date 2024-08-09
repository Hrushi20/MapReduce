use std::any::Any;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};
use std::{thread, usize};
use log::{error, info};
use common::json_rpc::RpcRequest;
use common::node::{Connection};
use common::serde_json;
use common::uuid::Uuid;

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
        let rpc_request = RpcRequest::from_json(&json).expect("Couldn't convert String to RpcRequest");
        println!("Json: {}", json);
        let hm = rpc_funcs.read().unwrap();
        let f = hm.get(&rpc_request.method).unwrap();
        let f = f.downcast_ref::<fn(Option<Vec<String>>, &Uuid)>().unwrap();
        f(rpc_request.params, &rpc_request.id);

        info!("Processed");
    }
}

fn register_rpc_fn() -> Arc<RwLock<HashMap<String, Box<dyn Any + Send + Sync>>>> {
    let mut hm:HashMap<String, Box<dyn Any + Send + Sync>> = HashMap::new();
    hm.insert(String::from("ping"), Box::new(ping as fn(Option<Vec<String>>, &Uuid)));
    hm.insert(String::from("execute_map"), Box::new(execute_map as fn(Option<Vec<String>>, &Uuid)));

    let rpc_funcs: Arc<RwLock<HashMap<String, Box<dyn Any + Send + Sync>>>> = Arc::new(RwLock::new(hm));
    rpc_funcs
}

fn ping(params: Option<Vec<String>>, id: &Uuid){
    println!("Logging Ping");
}

// Format for Params:
// Map Task Id, Task Start Idx, Task End Idx.
fn execute_map(params: Option<Vec<String>>, id: &Uuid){
    if params.is_none() {
        // return an error.
        return
    }

    let params = params.unwrap();
    println!("Executing Map Function");
    let test_file = "/Users/pc/test/huge_numbers.txt";
    let file = File::open(&test_file).unwrap();
    let mut buf_reader = BufReader::new(file);

    let mut data = vec![0u8; params[2].parse::<usize>().unwrap()];
    buf_reader.seek(SeekFrom::Start(params[1].parse::<u64>().unwrap())).unwrap();
    buf_reader.read_exact(&mut data).unwrap();

    let mut data_segment = 0u128; // Max supported 64 bytes.
    let mut shift = 0;
    // detect if String or

}
