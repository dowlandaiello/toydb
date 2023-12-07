use ascii_table::{Align, AsciiTable};
use clap::Parser;
use reqwest::Client;
use rustyline::{error::ReadlineError, DefaultEditor, Result};
use std::fmt::Display;
use toydb::{rpc::ExecuteQueryReq, types::table::LabeledTypedTuple};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = String::from("http://localhost:3000/api"))]
    rpc_addr: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Args::parse();

    // `()` can be used when no completer is required
    let mut rl = DefaultEditor::new()?;
    let mut active_db = String::from("system_catalogue");

    loop {
        let readline = rl.readline(format!("({}) >> ", active_db).as_str());
        match readline {
            Ok(line) => {
                // Check for database switches
                if line.starts_with("\\d ") {
                    active_db = line
                        .split(" ")
                        .nth(1)
                        .expect("malformed input command")
                        .to_owned();

                    continue;
                }

                if line.starts_with("\\help") {
                    println!("Execute SQL statements in this prompt. Use \\d <db_name> to change the active database. Use ^D to exit.");

                    continue;
                }

                // This is a SQL command. Send it
                let client = Client::new();
                let resp = match client
                    .post(cli.rpc_addr.as_str())
                    .json(&ExecuteQueryReq {
                        db_name: active_db.clone(),
                        query: line,
                    })
                    .send()
                    .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("failed to execute query: {:?}", e);
                        continue;
                    }
                };

                let res: Vec<Vec<LabeledTypedTuple>> = match resp.json().await {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("failed to execute query: {:?}", e);
                        continue;
                    }
                };

                for (i, rel) in res.into_iter().enumerate() {
                    println!("Output Relation #{}:", i + 1);

                    if rel.is_empty() {
                        println!("No tuples found.");

                        continue;
                    }

                    let mut output_table = AsciiTable::default();
                    output_table.set_max_width(rel[0].0.len());

                    // Add column names
                    for (i, (col_name, _)) in rel[0].0.iter().enumerate() {
                        output_table
                            .column(i)
                            .set_header(col_name)
                            .set_align(Align::Center);
                    }

                    output_table.print(
                        rel.iter()
                            .map(|tup| tup.flatten_display())
                            .collect::<Vec<Vec<&dyn Display>>>(),
                    );
                }
            }
            Err(ReadlineError::Interrupted) => break,
            Err(ReadlineError::Eof) => break,
            Err(err) => {
                panic!("{:?}", err);
            }
        }
    }

    Ok(())
}
