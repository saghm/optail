#[macro_use]
extern crate bson;
extern crate colored;
extern crate docopt;
extern crate mongodb;
extern crate rustc_serialize;

use std::io::{self, Write};
use std::thread;
use std::time::Duration;

use bson::Bson;
use colored::*;
use docopt::Docopt;
use mongodb::{Client, ThreadedClient};
use mongodb::db::ThreadedDatabase;
use mongodb::coll::options::{FindOptions, CursorType};

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

const USAGE: &'static str = "
Usage:
    optail [--host=<host>] [--port=<port>]
    optail (-h | --help)
    optail (-v | --version)

Options:
    -h --help      Show this screen.
    -v --version      Show version.
    --host=<host>  Host to connect to [default: localhost].
    --port=<port>  Port to connect to [default: 27017].
";

#[derive(Debug, RustcDecodable)]
struct Args {
    flag_host: String,
    flag_port: u16,
    flag_version: bool,
}

fn colorize(input: String) -> String {
    let open_br = format!("{}", "{".blue());
    let close_br = format!("{}", "}".blue());
    let colon = format!("{}", ":".blue());

    input.replace("{", &open_br).replace("}", &close_br).replace(":", &colon)
}

fn main() {
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.decode())
        .unwrap_or_else(|e| e.exit());

    if args.flag_version {
        println!("optail v{}", VERSION);
        return;
    }

    let mut stderr = io::stderr();

    let client = match Client::connect(&args.flag_host, args.flag_port) {
        Ok(c) => c,
        Err(e) => {
            writeln!(stderr, "{}", e).unwrap();
            return;
        }
    };

    let second = Duration::from_secs(1);

    let db = client.db("local");
    let oplog = db.collection("oplog.rs");

    let mut options = FindOptions::new();
    options.sort = Some(doc! { "ts" => (-1) });

    let timestamp = match oplog.find_one(None, Some(options)) {
        Ok(Some(ref last_entry)) if last_entry.contains_key("ts") => {
            last_entry.get("ts").unwrap().clone()
        }
        Ok(_) => Bson::I32(0),
        Err(e) => {
            writeln!(stderr, "{}", e).unwrap();
            return;
        }
    };

    let mut options = FindOptions::new();
    options.cursor_type = CursorType::TailableAwait;
    options.no_cursor_timeout = true;
    options.op_log_replay = true;

    let filter = doc! {
        "ts" => { "$gt" => timestamp }
    };

    let mut cursor = match oplog.find(Some(filter), Some(options)) {
        Ok(c) => c,
        Err(e) => {
            writeln!(stderr, "{}", e).unwrap();
            return;
        }
    };

    loop {
        while let Some(doc_result) = cursor.next() {
            let doc = match doc_result {
                Ok(d) => d,
                Err(e) => {
                    writeln!(stderr, "{}", e).unwrap();
                    return;
                }
            };

            let string = format!("{}", doc);
            println!("{}", colorize(string));
        }

        thread::sleep(second);
    }
}
