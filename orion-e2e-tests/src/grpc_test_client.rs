// Copyright 2025 The kmesh Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::net::SocketAddr;

use tonic::transport::Channel;

use crate::grpc_test_backend::test_proto::test_service_client::TestServiceClient;
use crate::grpc_test_backend::test_proto::{EchoRequest, EchoResponse};
use crate::Result;

pub struct GrpcTestClient {
    inner: TestServiceClient<Channel>,
}

impl GrpcTestClient {
    pub async fn connect(addr: SocketAddr) -> Result<Self> {
        let endpoint = format!("http://{addr}");
        let channel = Channel::from_shared(endpoint)
            .expect("valid endpoint")
            .connect()
            .await
            .map_err(|e| crate::Error::Http(e.to_string()))?;

        Ok(Self { inner: TestServiceClient::new(channel) })
    }

    pub async fn echo(&mut self, message: &str) -> Result<EchoResponse> {
        let request = tonic::Request::new(EchoRequest { message: message.to_owned() });
        let response = self.inner.echo(request).await.map_err(|e| crate::Error::Http(e.to_string()))?;
        Ok(response.into_inner())
    }

    pub async fn echo_backend_id(&mut self, message: &str) -> Result<String> {
        let response = self.echo(message).await?;
        Ok(response.backend_id)
    }
}
