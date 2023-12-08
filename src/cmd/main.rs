use clap::Parser;
use jsonrpc_v2::ResponseObject;
use prettytable::{Cell, Row, Table};
use reqwest::Client;
use rustyline::{
    error::ReadlineError,
    validate::{ValidationContext, ValidationResult, Validator},
    Completer, Editor, Helper, Highlighter, Hinter, Result, Result as LineResult,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt::Display;
use toydb::{rpc::ExecuteQueryReq, types::table::LabeledTypedTuple};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = String::from("http://localhost:3000/api"))]
    rpc_addr: String,
}

#[derive(Serialize, Deserialize)]
struct JsonRpcReq {
    jsonrpc: String,
    id: String,
    method: String,
    params: Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct JsonRpcError {
    code: i64,
    message: String,
    data: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct JsonRpcResp {
    jsonrpc: String,
    error: Option<JsonRpcError>,
    result: Option<Vec<Vec<LabeledTypedTuple>>>,
    id: String,
}

#[derive(Completer, Helper, Highlighter, Hinter)]
struct InputValidator {}

impl Validator for InputValidator {
    fn validate(&self, ctx: &mut ValidationContext) -> LineResult<ValidationResult> {
        let input = ctx.input();

        if input.starts_with('\\') {
            return Ok(ValidationResult::Valid(None));
        }

        if !input.ends_with(';') {
            Ok(ValidationResult::Incomplete)
        } else {
            Ok(ValidationResult::Valid(None))
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Args::parse();

    // `()` can be used when no completer is required
    let mut rl = Editor::new()?;
    let h = InputValidator {};
    rl.set_helper(Some(h));
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

                let exec_req = match serde_json::to_value(ExecuteQueryReq {
                    db_name: active_db.clone(),
                    query: line,
                }) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("failed to formulate query: {:?}", e);
                        continue;
                    }
                };

                let json_rpc_req = JsonRpcReq {
                    jsonrpc: "2.0".to_owned(),
                    id: "id".to_owned(),
                    method: "execute_query".to_owned(),
                    params: exec_req,
                };

                // This is a SQL command. Send it
                let client = Client::new();
                let resp = match client
                    .post(cli.rpc_addr.as_str())
                    .json(&json_rpc_req)
                    .send()
                    .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("failed to execute query: {:?}", e);
                        continue;
                    }
                };

                let res: JsonRpcResp = match resp.json().await {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("failed to execute query: {:?}", e);
                        continue;
                    }
                };

                let results = match res.result {
                    Some(res) => res,
                    _ => match res.error {
                        Some(e) => {
                            eprintln!("failed to execute query: {:?}", e);
                            continue;
                        }
                        _ => panic!("no result but no error"),
                    },
                };

                for (i, rel) in results.into_iter().enumerate() {
                    println!("Output Relation #{} (cardinality {}):", i + 1, rel.len());

                    if rel.is_empty() {
                        println!("No tuples found.");

                        continue;
                    }

                    let mut output_table = Table::new();

                    // Add column names
                    output_table.set_titles(Row::new(
                        rel[0]
                            .0
                            .iter()
                            .map(|(col_name, _)| Cell::new(col_name))
                            .collect::<Vec<Cell>>(),
                    ));

                    for tup in rel {
                        let repr = tup.flatten_display();
                        output_table.add_row(Row::new(
                            repr.into_iter()
                                .map(|repr| Cell::new(format!("{}", repr).as_str()))
                                .collect::<Vec<Cell>>(),
                        ));
                    }

                    output_table.printstd();
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
