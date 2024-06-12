use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use log::{error, info};
use common::node::{Connection};

pub fn start(connection: Connection){
    let listener = TcpListener::bind(connection.socket).expect("Unable to bind to Port");
    info!("Slave listening on PORT: {}", connection.get_socket().port());

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                info!("Request Received: {}", stream.local_addr().unwrap());
                thread::spawn(|| {
                    handle_client(stream)
                });
            }
            Err(e) => {
                error!("Connection failed: {}", e);
            }
        }
    }
}

fn handle_client(mut stream: TcpStream) {
    let mut buf = [0; 12];
    let ctr = 0;
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break, // Connection was closed by the client
            Ok(_) => {
                println!("Bytes: {:?}", std::str::from_utf8(&buf));

                let size = stream.write("Thanks".as_bytes()).unwrap();
                println!("Sent Reply: {} bytes", size);
            }
            Err(e) => {
                println!("Failed to read from stream: {}", e);
                break;
            }
        }
    }
}
