use std::hash::{Hash, Hasher};
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::str::FromStr;
use std::sync::Mutex;
use std::time::Duration;
use uuid::Uuid;

pub struct SlaveWriter {
    tcp_stream: Option<Mutex<TcpStream>>,
    id: Uuid,
    node_config: NodeConfig
}

impl SlaveWriter {
    pub fn new(node_config: NodeConfig) -> Self {
        let tcp_stream = Self::connect(&node_config);
        Self {
            tcp_stream,
            id: node_config.id,
            node_config
        }
    }

    fn connect(node_config: &NodeConfig) -> Option<Mutex<TcpStream>> {
        match TcpStream::connect_timeout(&node_config.socket, node_config.timeout) {
            Ok(tcp_stream) => {
                tcp_stream.set_write_timeout(Some(node_config.timeout)).expect("Failed to set write timeout on tcp stream");
                Some(Mutex::new(tcp_stream))
            },
            _ => None
        }
    }

    pub fn write_data(&mut self) -> Option<usize> {
        // Node is down, connect to server once again and try sending data.

        // Only one thread can write at a time.
        match self.tcp_stream.as_ref().unwrap().lock().unwrap().write("hi".as_bytes()) {
            Ok(size) =>  Some(size),
            Err(_) => None
        }
    }

    pub fn is_connected(&self) -> bool {
        match self.tcp_stream { Some(_) => true, _ => false }
    }

    pub fn ping(&self) -> Option<usize>{
        // Need to check if tcp stream is present?
        match self.tcp_stream.as_ref().unwrap().lock().unwrap().write("Hello_World".as_bytes()) {
            Ok(written_bytes) => Some(written_bytes),
            _ => None
        }
    }

    fn retry(&self) {

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
impl Eq for SlaveWriter{}

pub struct NodeConfig {
    socket: SocketAddr,
    id: Uuid,
    timeout: Duration
}

impl NodeConfig {
    pub fn new(ip_addr: &str, port: u16, timeout: Duration) -> Self {
        let ip_address = IpAddr::from_str(ip_addr).expect("Invalid Ip Address");
        let id = Uuid::new_v4();
        let socket = SocketAddr::new(ip_address, port);

        Self{
            socket,
            id,
            timeout
        }
    }

    pub fn get_socket(&self) -> SocketAddr {
        self.socket
    }
}

enum TaskType {
    MAP,
    REDUCE,
    IDLE
}

enum Status {
    Success,
    Fail
}