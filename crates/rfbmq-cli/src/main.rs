use std::io::Read;
use std::path::{Path, PathBuf};
use std::process;

use clap::{Parser, Subcommand};
use rfbmq_core::{
    parse_file, ClaimedMessage, Error, FsyncMode, Message, Priority, Queue, VERSION,
    DEFAULT_LEASE, DEFAULT_MAX_PENDING, DEFAULT_PURGE_AGE,
};

const EXIT_NOT_FOUND: i32 = 1;
const EXIT_ERROR: i32 = 2;

#[derive(Parser)]
#[command(name = "rfbmq", about = "File-based message queue")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a new queue
    Init {
        dir: PathBuf,
        /// Enable priority sub-directories
        #[arg(long)]
        priority: bool,
        /// Maximum pending messages
        #[arg(long, default_value_t = DEFAULT_MAX_PENDING)]
        max_pending: i64,
    },
    /// Push a message onto the queue
    Push {
        dir: PathBuf,
        /// Message priority
        #[arg(short, long)]
        priority: Option<Priority>,
        /// Message TTL in seconds
        #[arg(short, long)]
        ttl: Option<u32>,
        /// Correlation/tracing ID
        #[arg(short, long)]
        correlation_id: Option<String>,
        /// Reply queue path
        #[arg(short, long)]
        reply_to: Option<String>,
        /// Repeatable tag
        #[arg(short = 'T', long = "tag")]
        tags: Vec<String>,
        /// Repeatable dependency ID
        #[arg(short, long = "depends-on")]
        depends_on: Vec<String>,
        /// Override pid@hostname
        #[arg(short = 'b', long)]
        created_by: Option<String>,
        /// Skip all fsyncs
        #[arg(long)]
        no_fsync: bool,
        /// Defer dir fsyncs
        #[arg(long)]
        batch_fsync: bool,
        /// File to read body from (use - for stdin)
        file: Option<String>,
    },
    /// Pop a message from the queue
    Pop {
        dir: PathBuf,
    },
    /// Acknowledge a claimed message
    Ack {
        dir: PathBuf,
        path: String,
    },
    /// Negative-acknowledge a claimed message
    Nack {
        dir: PathBuf,
        path: String,
    },
    /// Print queue depth
    Depth {
        dir: PathBuf,
    },
    /// Reap expired leases and TTLs
    Reap {
        dir: PathBuf,
        /// Override lease timeout in seconds
        #[arg(short = 'l', long, default_value_t = DEFAULT_LEASE)]
        lease: u32,
        /// Skip TTL expiry check
        #[arg(long)]
        no_ttl: bool,
    },
    /// Purge old completed messages
    Purge {
        dir: PathBuf,
        /// Max age before purge in seconds
        #[arg(short, long, default_value_t = DEFAULT_PURGE_AGE)]
        age: u32,
    },
    /// Flush batch-mode dir fsyncs
    Sync {
        dir: PathBuf,
    },
    /// Print IDs of messages with satisfied dependencies
    Ready {
        dir: PathBuf,
    },
    /// Print human-readable message metadata
    Inspect {
        file: PathBuf,
    },
    /// Print message body only
    Cat {
        file: PathBuf,
    },
    /// Print version
    Version,
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init { dir, priority, max_pending } => cmd_init(&dir, priority, max_pending),
        Commands::Push {
            dir, priority, ttl, correlation_id, reply_to,
            tags, depends_on, created_by, no_fsync, batch_fsync, file,
        } => cmd_push(
            &dir, priority, ttl, correlation_id, reply_to,
            tags, depends_on, created_by, no_fsync, batch_fsync, file,
        ),
        Commands::Pop { dir } => cmd_pop(&dir),
        Commands::Ack { dir, path } => cmd_ack(&dir, &path),
        Commands::Nack { dir, path } => cmd_nack(&dir, &path),
        Commands::Depth { dir } => cmd_depth(&dir),
        Commands::Reap { dir, lease, no_ttl } => cmd_reap(&dir, lease, no_ttl),
        Commands::Purge { dir, age } => cmd_purge(&dir, age),
        Commands::Sync { dir } => cmd_sync(&dir),
        Commands::Ready { dir } => cmd_ready(&dir),
        Commands::Inspect { file } => cmd_inspect(&file),
        Commands::Cat { file } => cmd_cat(&file),
        Commands::Version => {
            println!("rfbmq {}", VERSION);
        }
    }
}

fn cmd_init(dir: &Path, priority: bool, max_pending: i64) {
    match Queue::init(dir, priority, max_pending) {
        Ok(_) => {}
        Err(e) => {
            eprintln!("rfbmq init: {}", e);
            process::exit(EXIT_ERROR);
        }
    }
}

fn cmd_push(
    dir: &Path,
    priority: Option<Priority>,
    ttl: Option<u32>,
    correlation_id: Option<String>,
    reply_to: Option<String>,
    tags: Vec<String>,
    depends_on: Vec<String>,
    created_by: Option<String>,
    no_fsync: bool,
    batch_fsync: bool,
    file: Option<String>,
) {
    let mut q = match Queue::open(dir) {
        Ok(q) => q,
        Err(e) => {
            eprintln!("rfbmq push: {}", e);
            process::exit(EXIT_ERROR);
        }
    };

    if no_fsync {
        q.set_fsync_mode(FsyncMode::None);
    } else if batch_fsync {
        q.set_fsync_mode(FsyncMode::Batch);
    }

    let body = match read_body(file) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("rfbmq push: {}", e);
            process::exit(EXIT_ERROR);
        }
    };

    let mut msg = Message::default();
    msg.body = body;

    if let Some(p) = priority {
        msg.header.priority = p;
    }
    if let Some(t) = ttl {
        msg.header.ttl = t;
    }
    msg.header.correlation_id = correlation_id;
    msg.header.reply_to = reply_to;
    if !tags.is_empty() {
        msg.header.tags = tags;
    }
    if !depends_on.is_empty() {
        msg.header.depends_on = depends_on
            .into_iter()
            .map(|s| s.parse().unwrap_or_else(|_| {
                eprintln!("rfbmq push: invalid dependency ID: {}", s);
                process::exit(EXIT_ERROR);
            }))
            .collect();
    }
    if let Some(b) = created_by {
        msg.header.created_by = b;
    }

    match q.enqueue(&mut msg) {
        Ok(id) => println!("{}", id),
        Err(Error::MaxPendingExceeded(_)) => {
            eprintln!("rfbmq push: queue full");
            process::exit(EXIT_ERROR);
        }
        Err(e) => {
            eprintln!("rfbmq push: {}", e);
            process::exit(EXIT_ERROR);
        }
    }
}

fn read_body(file: Option<String>) -> Result<String, std::io::Error> {
    match file {
        Some(f) if f != "-" => std::fs::read_to_string(&f),
        _ => {
            let mut buf = String::new();
            std::io::stdin().read_to_string(&mut buf)?;
            Ok(buf)
        }
    }
}

fn cmd_pop(dir: &Path) {
    let q = match Queue::open(dir) {
        Ok(q) => q,
        Err(e) => {
            eprintln!("rfbmq pop: {}", e);
            process::exit(EXIT_ERROR);
        }
    };

    match q.dequeue() {
        Ok(Some(claimed)) => println!("{}", claimed),
        Ok(None) => process::exit(EXIT_NOT_FOUND),
        Err(e) => {
            eprintln!("rfbmq pop: {}", e);
            process::exit(EXIT_ERROR);
        }
    }
}

fn cmd_ack(dir: &Path, path: &str) {
    let q = match Queue::open(dir) {
        Ok(q) => q,
        Err(e) => {
            eprintln!("rfbmq ack: {}", e);
            process::exit(EXIT_ERROR);
        }
    };

    let claimed = match ClaimedMessage::from_path(path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("rfbmq ack: {}", e);
            process::exit(EXIT_ERROR);
        }
    };

    if let Err(e) = q.complete(&claimed) {
        eprintln!("rfbmq ack: {}", e);
        process::exit(EXIT_ERROR);
    }
}

fn cmd_nack(dir: &Path, path: &str) {
    let q = match Queue::open(dir) {
        Ok(q) => q,
        Err(e) => {
            eprintln!("rfbmq nack: {}", e);
            process::exit(EXIT_ERROR);
        }
    };

    let claimed = match ClaimedMessage::from_path(path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("rfbmq nack: {}", e);
            process::exit(EXIT_ERROR);
        }
    };

    if let Err(e) = q.fail(&claimed) {
        eprintln!("rfbmq nack: {}", e);
        process::exit(EXIT_ERROR);
    }
}

fn cmd_depth(dir: &Path) {
    let q = match Queue::open(dir) {
        Ok(q) => q,
        Err(e) => {
            eprintln!("rfbmq depth: {}", e);
            process::exit(EXIT_ERROR);
        }
    };

    match q.depth() {
        Ok(n) => println!("{}", n),
        Err(e) => {
            eprintln!("rfbmq depth: {}", e);
            process::exit(EXIT_ERROR);
        }
    }
}

fn cmd_reap(dir: &Path, lease: u32, no_ttl: bool) {
    let mut q = match Queue::open(dir) {
        Ok(q) => q,
        Err(e) => {
            eprintln!("rfbmq reap: {}", e);
            process::exit(EXIT_ERROR);
        }
    };

    q.set_lease_timeout(lease);

    match q.reap() {
        Ok(n) => eprintln!("reaped {} lease(s)", n),
        Err(e) => {
            eprintln!("rfbmq reap: {}", e);
            process::exit(EXIT_ERROR);
        }
    }

    if !no_ttl {
        match q.reap_ttl() {
            Ok(n) => eprintln!("reaped {} TTL-expired message(s)", n),
            Err(e) => {
                eprintln!("rfbmq reap: {}", e);
                process::exit(EXIT_ERROR);
            }
        }
    }
}

fn cmd_purge(dir: &Path, age: u32) {
    let q = match Queue::open(dir) {
        Ok(q) => q,
        Err(e) => {
            eprintln!("rfbmq purge: {}", e);
            process::exit(EXIT_ERROR);
        }
    };

    match q.purge(age) {
        Ok(n) => eprintln!("purged {} message(s)", n),
        Err(e) => {
            eprintln!("rfbmq purge: {}", e);
            process::exit(EXIT_ERROR);
        }
    }
}

fn cmd_sync(dir: &Path) {
    let mut q = match Queue::open(dir) {
        Ok(q) => q,
        Err(e) => {
            eprintln!("rfbmq sync: {}", e);
            process::exit(EXIT_ERROR);
        }
    };

    q.set_fsync_mode(FsyncMode::Batch);

    if let Err(e) = q.sync() {
        eprintln!("rfbmq sync: {}", e);
        process::exit(EXIT_ERROR);
    }
}

fn cmd_ready(dir: &Path) {
    let q = match Queue::open(dir) {
        Ok(q) => q,
        Err(e) => {
            eprintln!("rfbmq ready: {}", e);
            process::exit(EXIT_ERROR);
        }
    };

    match q.list_ready() {
        Ok(ids) => {
            if ids.is_empty() {
                process::exit(EXIT_NOT_FOUND);
            }
            for id in &ids {
                println!("{}", id);
            }
        }
        Err(e) => {
            eprintln!("rfbmq ready: {}", e);
            process::exit(EXIT_ERROR);
        }
    }
}

fn cmd_inspect(file: &Path) {
    let msg = match parse_file(file) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("rfbmq inspect: {}", e);
            process::exit(EXIT_ERROR);
        }
    };

    let h = &msg.header;
    let id = h.id.as_ref().map(|id| id.to_string()).unwrap_or_default();
    println!("Id:             {}", id);
    println!("Created-At:     {}", h.created_at);
    println!("Created-By:     {}", h.created_by);
    println!("Priority:       {}", h.priority);
    println!("Retry-Count:    {}", h.retry_count);
    println!("TTL:            {}", h.ttl);
    if !h.tags.is_empty() {
        println!("Tags:           {}", h.tags.join(", "));
    }
    if let Some(ref cid) = h.correlation_id {
        println!("Correlation-Id: {}", cid);
    }
    if let Some(ref rt) = h.reply_to {
        println!("Reply-To:       {}", rt);
    }
    if !h.depends_on.is_empty() {
        let dep_str: Vec<&str> = h.depends_on.iter().map(|id| id.as_str()).collect();
        println!("Depends-On:     {}", dep_str.join(", "));
    }
    if !h.custom.is_empty() {
        println!("Custom:");
        for line in &h.custom {
            println!("  {}", line);
        }
    }
}

fn cmd_cat(file: &Path) {
    let msg = match parse_file(file) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("rfbmq cat: {}", e);
            process::exit(EXIT_ERROR);
        }
    };

    print!("{}", msg.body);
}
