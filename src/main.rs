use std::time::{Duration, Instant};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, RwLockWriteGuard};
use tokio::time::{self, Interval};

#[derive(Clone, Debug)]
struct ValueEntry {
  data: String,
  expires_at: Option<Instant>,
}

// Define a convenient type alias for our shared thread-safe database
type Db = Arc<RwLock<HashMap<String, ValueEntry>>>;

#[derive(Debug, PartialEq)]
enum Command {
  Ping,
  Get { key: String },
  Set { key: String, val: String, ttl_secs: Option<u64> },
  Del { key: String },
}

impl Command {
  fn from_line(line: &str) -> Result<Command, String> {
    let parts: Vec<&str> = line.trim().split_whitespace().collect();
    
    if parts.is_empty() {
      return Err("Empty command".to_string());
    }
    
    let cmd_name: String = parts[0].to_uppercase();

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
        if parts.len() >= 3 {
          let key: String = parts[1].to_string();

          if parts.len() >= 5 && parts[parts.len() - 2] .to_uppercase()== "EX" {
            let ttl_str: &str = parts[parts.len() - 1];
            let ttl_secs: u64 = ttl_str
              .parse::<u64>()
              .map_err(|_| "EX value must be a number".to_string())?;

            let val: String = parts[2..parts.len() - 2].join(" ");

            Ok(
              Command::Set{
                key,
                val,
                ttl_secs: Some(ttl_secs),
              }
            )
          } else {
            let val: String = parts[2..].join(" ");

            Ok(
              Command::Set{
                key,
                val,
                ttl_secs: None,
              }
            )
          }
        } else {
          return Err("Usage: Set <key> <value> [EX seconds]".to_string());
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
  let addr: &str = "127.0.0.1:8765";
  // let addr = "localhost:8765";
  let listener: TcpListener = TcpListener::bind(addr).await?;

  println!("Server listening on: {}", addr);

  // Initialize our shared in-memory database
  let db: Db = Arc::new(RwLock::new(HashMap::new()));

  // Spawn a background janitor task to purge expired keys periodically
  let janitor_db: Arc<RwLock<HashMap<String, ValueEntry>>> = Arc::clone(&db);
  tokio::spawn(async move {
    let mut interval: Interval = time::interval(Duration::from_secs(5));

    loop {
      interval.tick().await;

      let now: Instant = Instant::now();
      let mut store: RwLockWriteGuard<'_, HashMap<String, ValueEntry>> = janitor_db.write().await;

      // Retain only entries that have not expired
      store.retain(
        |_, entry: &mut ValueEntry| match entry.expires_at {
          Some(exp) => exp > now,
          None => true
        }
      );
    }
  });

  loop {
    // Accept incoming TCP connections
    let (socket, client_addr) = listener.accept().await?;
    println!("New client connected: {}", client_addr);

    // Clone the arc pointer for this specific client task
    let db_clone: Arc<RwLock<HashMap<String, ValueEntry>>> = Arc::clone(&db);

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
  let mut buf_reader: BufReader<tokio::net::tcp::ReadHalf<'_>> = BufReader::new(reader);
  let mut line: String = String::new();

  // Read lines until client disconnects
  while buf_reader.read_line(&mut line).await? > 0 {
    let response: String = match Command::from_line(&line) {
      Ok(cmd) => match cmd {
        Command::Ping => "+PONG\r\n".to_string(),

        Command::Set { key, val, ttl_secs } => {
          let expires_at: Option<Instant> = ttl_secs.map(|s| Instant::now() + Duration::from_secs(s));
          let entry: ValueEntry = ValueEntry {
            data: val,
            expires_at,
          };

          // Acquire exclusive write lock
          let mut store: RwLockWriteGuard<'_, HashMap<String, ValueEntry>> = db.write().await;
          store.insert(key, entry);

          "+OK\r\n".to_string()
        },

        Command::Get { key } => {
          // Acquire shared read lock
          let mut store: RwLockWriteGuard<'_, HashMap<String, ValueEntry>> = db.write().await;
          // Passive expiration check on GET
          if let Some(entry) = store.get(&key) {
            if let Some(exp) = entry.expires_at {
              if Instant::now() >= exp {
                store.remove(&key);
                "$-1\r\n".to_string()
              } else {
                format!("{}\r\n{}\r\n", entry.data.len(), entry.data)
              }
            } else {
              format!("{}\r\n{}\r\n", entry.data.len(), entry.data)
            }
          } else {
            "$-1\r\n".to_string()
          }
        },

        Command::Del { key } => {
          // Acquire exclusive write lock
          let mut store: RwLockWriteGuard<'_, HashMap<String, ValueEntry>> = db.write().await;
          let deleted: Option<ValueEntry> = store.remove(&key);

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
