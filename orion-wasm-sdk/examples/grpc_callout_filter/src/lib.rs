//! Example using the Orion Wasm SDK to perform a gRPC Callout
use orion_wasm_sdk::{
    dispatch_grpc_call, init_tracing, orion_plugin, FilterAction, HttpHeaders, Plugin, RequestHandle, http, bytes
};
use orion_wasm_types::GrpcCalloutRequest;
use prost::Message;
use smol_str::SmolStr;
use tracing::{error, info};

pub mod test_service {
    include!(concat!(env!("OUT_DIR"), "/orion.test.rs"));
}

#[derive(Default)]
struct GrpcCalloutFilter;

#[orion_plugin]
impl Plugin for GrpcCalloutFilter {
    fn on_plugin_start(&mut self) {
        let _ = init_tracing();
        info!(version = "1.0", "GrpcCalloutFilter Wasm: Instance initialized!");
    }

    fn on_request_headers(&mut self, ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        info!("--- Processing Request Headers with gRPC Callout ---");

        let req = test_service::EchoRequest {
            message: "Hello from Wasm!".to_string(),
        };

        let mut buf = Vec::new();
        req.encode(&mut buf).unwrap();

        let callout_req = GrpcCalloutRequest {
            cluster_name: SmolStr::new("service"),
            service_name: SmolStr::new("orion.test.TestService"),
            method_name: SmolStr::new("Echo"),
            initial_metadata: vec![(SmolStr::new("x-callout-id"), SmolStr::new("wasm-grpc-123"))],
            message: bytes::Bytes::from(buf),
        };

        match dispatch_grpc_call(&callout_req) {
            Ok(response) => {
                if response.status != 0 {
                    let msg = format!("gRPC Error: {}", response.status_message);
                    return ctx.direct_response(http::Response::builder().status(500).body(bytes::Bytes::from(msg)).unwrap());
                }

                match test_service::EchoResponse::decode(&response.message[..]) {
                    Ok(echo_res) => {
                        let msg = format!("grpc callout success: {}", echo_res.message);
                        return ctx.direct_response(http::Response::builder().status(200).body(bytes::Bytes::from(msg)).unwrap());
                    }
                    Err(e) => {
                        let msg = format!("gRPC Decode Error: {}", e);
                        return ctx.direct_response(http::Response::builder().status(500).body(bytes::Bytes::from(msg)).unwrap());
                    }
                }
            }
            Err(e) => {
                error!("Failed to dispatch gRPC call: {:?}", e);
                return ctx.direct_response(http::Response::builder().status(500).body(bytes::Bytes::from_static(b"Failed to dispatch")).unwrap());
            }
        }
    }
}
