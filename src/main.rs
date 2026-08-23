use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

// Define a convenient type alias for our shared thread-safe database
type Db = Arc<RwLock<HashMap<String, String>>>;

#[derive(Debug, PartialEq)]
enum Command {
  Ping,
  Get { key: String },
  Set { key: String, val: String },
  Del { key: String },
}

impl Command {
  fn from_line(line: &str) -> Result<Command, String> {
    let parts: Vec<&str> = line.trim().split_whitespace().collect();
    
    if parts.is_empty() {
      return Err("Empty command".to_string());
    }
    
    let cmd_name = parts[0].to_uppercase();

    match cmd_name.as_str() {
      "PING" => Ok(Command::Ping),
      "GET" => {
        if parts.len() == 2 {
          Ok(
            Command::Get {
              key: parts[1].to_string()
            }
          )
        } else {
            return Err("Usage: GET <key>".to_string());
        }
      }
      "SET" => {
        if parts.len() == 3 {
          let val = parts[2..].join(" ");

          Ok(
            Command::Set {
              key: parts[1].to_string(),
              val,
            }
          )
        } else {
          return Err("Usage: Set <key> <value>".to_string());
        }
      }
      "DEL" => {
        if parts.len() == 2 {
          Ok(
            Command::Del {
              key: parts[1].to_string()
            }
          )
        } else {
          return Err("Usage: DEL <key>".to_string());
        }
      }
      _ => Err(format!("Unknown command: '{}'", parts[0])),
    }
  }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
  let addr = "127.0.0.1:8765";
  // let addr = "localhost:8765";
  let listener: TcpListener = TcpListener::bind(addr).await?;

  println!("Server listening on: {}", addr);

  // Initialize our shared in-memory database
  let db: Db = Arc::new(RwLock::new(HashMap::new()));

  loop {
    // Accept incoming TCP connections
    let (socket, client_addr) = listener.accept().await?;
    println!("New client connected: {}", client_addr);

    // Clone the arc pointer for this specific client task
    let db_clone: Arc<RwLock<HashMap<String, String>>> = Arc::clone(&db);

    // Spawn a new task for each client connection
    tokio::spawn(async move {
      if let Err(e) = handle_client(socket, db_clone).await {
        eprintln!("Error handling client {}: {:?}", client_addr, e);
      }

      println!("Client disconnected: {}", client_addr);
    });
  }
}

async fn handle_client(mut socket: TcpStream, db: Db) -> Result<(), Box<dyn Error>> {
  let (reader, mut writer) = socket.split();
  let mut buf_reader = BufReader::new(reader);
  let mut line = String::new();

  // Read lines until client disconnects
  while buf_reader.read_line(&mut line).await? > 0 {
    let response: String = match Command::from_line(&line) {
      Ok(cmd) => match cmd {
        Command::Ping => "+PONG\r\n".to_string(),

        Command::Set { key, val } => {
          // Acquire exclusive write lock
          let mut store = db.write().await;
          store.insert(key, val);

          "+OK\r\n".to_string()
        },

        Command::Get { key } => {
          // Acquire shared read lock
          let store = db.read().await;

          match store.get(&key) {
            Some(val) => format!("{}\r\n{}\r\r", val.len(), val),
            None => "$-1\r\n".to_string(),
          }
        },

        Command::Del { key } => {
          // Acquire exclusive write lock
          let mut store = db.write().await;
          let deleted = store.remove(&key);

          match deleted {
              Some(_) => ":1\r\n".to_string(),
              None => ":0\r\n".to_string(),
          }
        },
      },

      Err(err) => format!("-ERR {}\r\n", err),
    };

    writer.write_all(response.as_bytes()).await?;
    line.clear();
  }

  Ok(())
}
