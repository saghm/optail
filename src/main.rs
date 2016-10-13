#[macro_use]
extern crate bson;
extern crate colored;
extern crate docopt;
extern crate mongodb;
extern crate rustc_serialize;

#[macro_use]
mod macros;

use std::io::{self, Write};
use std::thread;
use std::time::Duration;

use bson::Bson;
use colored::*;
use docopt::Docopt;
use mongodb::{Client, ThreadedClient};
use mongodb::coll::options::{FindOptions, CursorType};
use mongodb::cursor::Cursor;
use mongodb::db::ThreadedDatabase;
use mongodb::error::Error as MongoError;

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

const USAGE: &'static str = "
Usage:
    optail [--host=<host>] [--port=<port>] [--debug]
    optail (-h | --help)
    optail (-v | --version)

Options:
    -h --help      Show this screen.
    -v --version   Show version.
    --host=<host>  Host to connect to [default: localhost].
    --port=<port>  Port to connect to [default: 27017].
    --debug        Print debug information.
";

#[derive(Debug, RustcDecodable)]
struct Args {
    flag_host: String,
    flag_port: u16,
    flag_version: bool,
    flag_debug: bool,
}

fn colorize(input: String) -> String {
    let open_br = format!("{}", "{".blue());
    let close_br = format!("{}", "}".blue());
    let colon = format!("{}", ":".blue());

    input.replace("{", &open_br).replace("}", &close_br).replace(":", &colon)
}

fn get_timestamp(client: Client) -> Result<Bson, MongoError> {
    let db = client.db("local");
    let oplog = db.collection("oplog.rs");

    let mut options = FindOptions::new();
    options.sort = Some(doc! { "ts" => (-1) });

    match oplog.find_one(None, Some(options)) {
        Ok(Some(ref last_entry)) if last_entry.contains_key("ts") => {
            Ok(last_entry.get("ts").unwrap().clone())
        }
        Ok(_) => Ok(Bson::I32(0)),
        Err(e) => Err(e),
    }
}

fn run_loop(mut cursor: Cursor, debug: bool) {
    let mut stderr = io::stderr();

    let second = Duration::from_secs(1);

    loop {
        while let Some(doc_result) = cursor.next() {
            let doc = get_or_fail!(doc_result, stderr, debug);

            if let Some(val) = doc.get("$err") {
                fail!(stderr, format!("got error {}", val), true);
            }

            let string = format!("{}", doc);
            println!("{}", colorize(string));
        }

        thread::sleep(second);
    }
}

fn tail_oplog(host: &str, port: u16, debug: bool) {
    let mut stderr = io::stderr();

    let client = get_or_fail!(Client::connect(host, port), stderr, debug);
    let db = client.db("local");
    let oplog = db.collection("oplog.rs");

    let timestamp = get_or_fail!(get_timestamp(client), stderr, debug);

    let mut options = FindOptions::new();
    options.cursor_type = CursorType::TailableAwait;
    options.no_cursor_timeout = true;
    options.op_log_replay = true;

    let filter = doc! {
        "ts" => { "$gt" => timestamp }
    };

    let cursor = get_or_fail!(oplog.find(Some(filter), Some(options)), stderr, debug);

    run_loop(cursor, debug)
}

fn main() {
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.decode())
        .unwrap_or_else(|e| e.exit());

    if args.flag_version {
        println!("optail v{}", VERSION);
        return;
    }

    tail_oplog(&args.flag_host, args.flag_port, args.flag_debug)
}
