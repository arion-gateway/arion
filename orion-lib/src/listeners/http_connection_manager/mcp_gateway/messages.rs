use super::model::{JsonRpcMessage, JsonRpcRequest};
use crate::listeners::http_filters::FilterDecision;
use bytes::Bytes;
use std::error::Error;

pub fn handle_message(body: Bytes) -> Result<FilterDecision, Box<dyn Error>> {
    todo!()
    //let message: JsonRpcMessage = serde_json::from_str(body)?;

    //match message {
    //    JsonRpcMessage::Request(req) => {
    //        let response = dispatch_request(req)?;
    //        Ok(Some(serde_json::to_string(&response)?))
    //    },

    //    JsonRpcMessage::Notification(notif) => {
    //        match notif.method.as_str() {
    //            "notifications/initialized" => {
    //                println!("Handshake completato!");
    //            },
    //            "$/cancelRequest" => {
    //            },
    //            _ => {}
    //        }
    //        Ok(None)
    //    },

    //    JsonRpcMessage::Response(res) => {
    //        Ok(None)
    //    },

    //    JsonRpcMessage::Error(err) => {
    //        Ok(None)
    //    }
    //}
}

fn dispatch_request(request: JsonRpcRequest) -> Result<String, Box<dyn Error>> {
    todo!()
    //match request.method.as_str() {
    //    "initialize" => handle_initialize(request),
    //    "tools/call" => {
    //        // Handle tool calls...
    //        Ok("Tool call handled".to_string())
    //    }
    //    _ => Err(format!("Method '{}' not found", request.method).into()),
    //}
}
