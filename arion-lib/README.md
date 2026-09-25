## What does it do 
It is more Envoy

# Running
Default bootstrap config:
>  
> ### Listeners:  
> - server-1-tls ----> domains [127.0.0.1 or wildcard ] ----> path / -----> cluster selector local\_cluster  
> - server-1-tls ----> domains [127.0.0.1 or wildcard ] ----> path /proxy ----> cluster selector remote\_cluster   
> - server-2-plaintext ----> routes [wildcard ] ----> path / -----> cluster selector local\_cluster  
> - server-2-plaintext ----> routes [wildcard ] ----> path /ignored -----> cluster selector local\_cluster  
>
> ### Clusters:  
> - cluster local-srv/local\_cluster STATIC load balance endpoints [127.0.0.1:5000. 127.0.0.1:4000]  
> - cluster remove-srv/local\_cluster STATIC load balance endpoints [127.0.0.1:5000. 127.0.0.1:4000]  
> - cluster proxy-srv/remote\_cluster STATIC load balance endpoints [127.0.0.1:6000]  
>
>


## Generate self sign certs:
```bash
openssl req -x509 -sha256 -nodes -days 365 -newkey rsa:2048 -keyout self_signed_certs/key.pem -out self_signed_certs/cert.pem
```

## Run downstream/backend servers__

```bash
cd ng3_proxy/tools/
cd simple_server 
cargo build --release
cd ..
./release_start_clients.sh
```

## Run proxy
```bash
cargo run  -p arion-proxy -- --config arion-proxy/conf/arion-runtime.yaml --with-envoy-bootstrap arion-proxy/conf/arion-bootstrap.yaml 

```

## Run upstream/client  
```bash
curl -k -H "host: http://127.0.0.1" http://127.0.0.1:8000  
curl -kvi https://127.0.0.1:8000/
curl -vki http://127.0.0.1:8001/

curl -vik --resolve example.com:8443:127.0.0.1 https://example.com:8443/proxy
curl -vik --resolve dublin_1.irc.huawei.com:8443:127.0.0.1 https://dublin_1.irc.huawei.com:8443
curl -vik --resolve dublin_1.beefcake.com:8443:127.0.0.1 https://dublin_1.beefcake.com:8443

curl -vik --resolve dublin_1.irc.huawei.com:9443:127.0.0.1 https://dublin_1.irc.huawei.com:9443
curl -vik --resolve dublin_1.beefcake.com:9443:127.0.0.1 https://dublin_1.beefcake.com:9443


 
curl -vik --cacert test_certs/beefcake.intermediate.ca-chain.cert.pem --cert test_certs/beefcakeCA-gathered/beefcake-dublin.cert.pem --key test_certs/beefcakeCA-gathered/beefcake-dublin.key.pem --resolve dublin_1.beefcake.com:9443:127.0.0.1 https://dublin_1.beefcake.com:9443

curl -vik --cacert test_certs/beefcakeCA-gathered/beefcake-dublin.cert.pem --cert test_certs/beefcakeCA-gathered/beefcake-dublin.cert.pem --key test_certs/beefcakeCA-gathered/beefcake-dublin.key.pem --resolve dublin_1.beefcake.com:9443:127.0.0.1 https://dublin_1.beefcake.com:9443

curl -vi --cacert test_certs/beefcakeCA-gathered/beefcake.intermediate.ca-chain.cert.pem  --cert test_certs/beefcakeCA-gathered/beefcake-dublin.cert.pem --key test_certs/beefcakeCA-gathered/beefcake-dublin.key.pem --resolve athlone_2.beefcake.com:8443:127.0.0.1 https://athlone_2.beefcake.com:8443
 
 
curl -vik -H "xd-req-remove-route: dummy" -H "xd-req-remove-vh: dummy" -H "xd-resp-pass-vh: dummy" -H "xd-req-pass-vh: dummy"  --resolve example.com:8443:127.0.0.1 https://example.com:8443/proxy


// this will return an error directly from the simple_server since simple server doesn't understand TLS
curl -vik -H "xd-req-remove-route: dummy" -H "xd-req-remove-vh: dummy" -H "xd-resp-pass-vh: dummy" -H "xd-req-pass-vh: dummy"  --resolve blah.com:8001:127.0.0.1 https://blah.com:8001 
