use std::hash::{Hash, Hasher};
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::str::FromStr;
use std::sync::Mutex;
use std::time::Duration;
use uuid::Uuid;
use crate::json_rpc::RpcRequest;

pub struct SlaveWriter {
    tcp_stream: Option<Mutex<TcpStream>>,
    id: Uuid,
    connection: Connection,
}

impl SlaveWriter {
    pub fn new(connection: Connection) -> Self {
        let tcp_stream = Self::connect(&connection);
        Self {
            tcp_stream,
            id: connection.id,
            connection,
        }
    }

    fn connect(connection: &Connection) -> Option<Mutex<TcpStream>> {
        match TcpStream::connect_timeout(&connection.socket, connection.timeout) {
            Ok(tcp_stream) => {
                tcp_stream
                    .set_write_timeout(Some(connection.timeout))
                    .expect("Failed to set write timeout on tcp stream");
                Some(Mutex::new(tcp_stream))
            }
            _ => None,
        }
    }

    pub fn write_data(&mut self) -> Option<usize> {
        // Node is down, connect to server once again and try sending data.

        // Only one thread can write at a time.
        match self
            .tcp_stream
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .write("hi".as_bytes())
        {
            Ok(size) => Some(size), // Need to add code which waits for ack.
            Err(_) => None,
        }
    }

    pub fn is_connected(&self) -> bool {
        match self.tcp_stream {
            Some(_) => true,
            _ => false,
        }
    }

    pub fn ping(&self) -> Option<bool> {

        if let Some(stream) = self.tcp_stream.as_ref() {
            let mut tcp_stream = stream.lock().expect("Error locking mutex");

            let rpc_request = RpcRequest::new(String::from("ping"), None);
            let rpc_request_bytes = serde_json::to_vec(&rpc_request).expect("Cannot convert struct to json");

            return match tcp_stream.write_all(&*rpc_request_bytes) {
                Ok(..) => {
                    tcp_stream.flush();
                    Some(true)
                },
                _ => None
            };
        };
        return None;
    }

    pub fn retry(&mut self) {
        let mut i = 0;

        while i < 3 {
            let tcp_stream = Self::connect(&self.connection);
            if tcp_stream.is_some() {
                self.tcp_stream = tcp_stream;
                break;
            }
            i += 1;
        }
    }
}

impl Hash for SlaveWriter {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PartialEq<Self> for SlaveWriter {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for SlaveWriter {}

pub struct Connection {
    pub socket: SocketAddr,
    id: Uuid,
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

enum TaskType {
    MAP,
    REDUCE,
    IDLE,
}

enum Status {
    Success,
    Fail,
}
