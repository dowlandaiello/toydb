use std::io::Result;

fn main() -> Result<()> {
    capnpc::CompilerCommand::new()
        .src_prefix("schema")
        .file("schema/btree.capnp")
        .run()
        .expect("compiler command");
    prost_build::compile_protos(&["src/items.proto"], &["src/"])?;
    Ok(())
}
