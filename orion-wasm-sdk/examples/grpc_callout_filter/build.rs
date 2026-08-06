fn main() {
    prost_build::compile_protos(&["src/test_service.proto"], &["src/"]).unwrap();
}
