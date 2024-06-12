struct RpcRequest {
    function_name: String,
    params: Vec<String>,
}

struct RpcResponse {
    function_name: String,
    result: Vec<u8>,
}
