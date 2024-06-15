use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::utils;

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcRequest {
    json_rpc: String,
    method: String,
    params: Option<Vec<String>>,
    id: Uuid
}

impl RpcRequest {
   pub fn new(method: String, params: Option<Vec<String>>) -> Self {
      Self {
          json_rpc: String::from("2.0"),
          method,
          params,
          id: Uuid::new_v4()
      }
   }

   pub fn from_json(json: String) -> Option<Self> {
       match serde_json::to_value(&json){
           Ok(value) => serde_json::from_value(value).unwrap(),
           Err(_) => None
       }
   }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcResponse {
    json_rpc: String,
    result: Option<Vec<String>>,
    error: Option<RpcError>,
    id: Uuid
}

impl RpcResponse {
    pub fn new(result: Option<Vec<String>>, error: Option<RpcError>, id: Uuid) -> Self {
       Self{
          json_rpc: String::from("2.0"),
          result,
          error,
          id
       }
    }

    pub fn from_json(json: String) -> Option<Self> {
        match serde_json::to_value(&json){
            Ok(value) => serde_json::from_value(value).unwrap(),
            Err(_) => None
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcError {
   code: RpcErrorCode,
   message: String
}

impl RpcError {
   pub fn new(code: RpcErrorCode, message: String) -> Self {
       Self{
          code,
          message
       }
   }

    pub fn from_json(json: String) -> Option<Self> {
        match serde_json::to_value(&json){
            Ok(value) => serde_json::from_value(value).unwrap(),
            Err(_) => None
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcErrorCode {
    ParseError = -32700,
    InvalidRequest = -32600,
    MethodNotFound = -32601,
    InvalidParams = -32602,
    InternalError = -32603,
    ServerError = -32000
}

