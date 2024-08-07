use std::hash::{Hash, Hasher};
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Mutex;
use std::time::Duration;
use log::error;
use uuid::Uuid;
use crate::json_rpc::RpcRequest;

pub struct SlaveWriter<'a> {
    pub id: Uuid,
    connection: &'a Connection,
}

impl<'a> SlaveWriter<'a> {
    pub fn new(connection: &'a Connection) -> Self {
        Self {
            id: connection.id,
            connection,
        }
    }

    fn connect(connection: &Connection) -> Option<TcpStream> {
        match TcpStream::connect_timeout(&connection.socket, connection.timeout) {
            Ok(tcp_stream) => {
                tcp_stream
                    .set_write_timeout(Some(connection.timeout))
                    .expect("Failed to set write timeout on tcp stream");
                Some(tcp_stream)
            }
            _ => None,
        }
    }

    pub fn write_request(&mut self, data: &RpcRequest) -> bool {
        // Node is down, connect to server once again and try sending data.

        // Only one thread can write at a time.
        match Self::connect(&self.connection).unwrap()
            .write_all(&*serde_json::to_vec(data).expect("Can't send Request."))
        {
            Ok(..) => true, // Need to add code which waits for ack.
            Err(_) => false,
        }
    }

    pub fn ping(&mut self) -> Option<bool> {

        let mut result = false;
        if let Some(mut tcp_stream) = Self::connect(&self.connection) {
            let rpc_request = RpcRequest::new(String::from("ping"), None);
            let rpc_request_bytes = serde_json::to_vec(&rpc_request).expect("Cannot convert struct to json");

            result =  match tcp_stream.write_all(&*rpc_request_bytes) {
                Ok(..) => {
                    tcp_stream.flush().expect("TODO: panic message");
                    true
                },
                _ => {
                    false
                }
            };
        };

        if (result){
          return Some(true);
        }
        // There is a lag in connection. Not sure whyyy? Need to debug.
        // Logic looks stupid.
        if self.retry() {
           return Some(true);
        } // Next time the state gets updated.
        return None;
    }

    pub fn retry(&mut self) -> bool {
        let mut i = 0;

        while i < 3 {
            let tcp_stream = Self::connect(&self.connection);
            if tcp_stream.is_some() {
                return true;
            }
            i += 1;
        }
        return false;
    }
}

impl Hash for SlaveWriter<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PartialEq<Self> for SlaveWriter<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for SlaveWriter<'_> {}

#[derive(Copy, Clone, Debug)]
pub struct Connection {
    pub socket: SocketAddr,
    pub id: Uuid,
    timeout: Duration,
}

impl Connection {
    pub fn new(ip_addr: &str, port: u16, timeout: Duration) -> Self {
        let ip_address = IpAddr::from_str(ip_addr).expect("Invalid Ip Address");
        let id = Uuid::new_v4();
        let socket = SocketAddr::new(ip_address, port);

        Self {
            socket,
            id,
            timeout,
        }
    }

    pub fn get_socket(&self) -> SocketAddr {
        self.socket
    }
}

pub struct NodeHealth {
    pub id: Uuid,
    pub state: NodeStatus,
}

impl NodeHealth {
    pub fn new(id: Uuid, state: NodeStatus) -> Self {
        Self {
           id,
           state
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum NodeStatus {
    Idle,
    Processing,
    Dead,
}
