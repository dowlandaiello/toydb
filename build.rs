fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("schema")
        .file("schema/items.capnp")
        .run()
        .expect("schema compiler command");
}
