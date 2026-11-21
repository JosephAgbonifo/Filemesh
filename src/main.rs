mod identity;
mod libp2p_peers;
mod manifest;
mod protocol;
mod room;
mod store;
mod util;

use anyhow::Result;
use clap::{Parser, Subcommand};
use colored::Colorize;
use libp2p::Multiaddr;
use std::{io::Write, path::PathBuf};
use tokio::sync::mpsc;

#[derive(Parser, Debug)]
#[command(name = "FileMesh-RS")]
#[command(about = "Peer-to-peer file transfer using libp2p", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start a peer and join a room
    Start {
        /// Peer name
        #[arg(short, long)]
        name: String,

        /// Room name to join
        #[arg(short, long)]
        room: String,

        /// Directory where received files are saved
        #[arg(short, long, default_value = "received_files")]
        download_dir: PathBuf,

        /// Download incoming files without asking (use only in rooms you trust)
        #[arg(long)]
        auto_accept: bool,

        /// Identity key file (default: ~/.filemesh/<name>.key, created on first run)
        #[arg(long, conflicts_with = "ephemeral")]
        key_file: Option<PathBuf>,

        /// Use a throw-away identity for this run instead of a saved one
        #[arg(long)]
        ephemeral: bool,

        /// Address to listen on
        #[arg(long, default_value = "/ip4/0.0.0.0/tcp/0")]
        listen: Multiaddr,

        /// Dial a peer at startup, e.g. /ip4/203.0.113.7/tcp/4001 (repeatable).
        /// Use this to join a room beyond your local network.
        #[arg(long = "connect")]
        connect: Vec<Multiaddr>,
    },
}

#[derive(Debug, Clone)]
pub enum UserCommand {
    ListPeers,
    SendFile {
        file_path: String,
        /// A peer name or full PeerId.
        target: Option<String>,
        broadcast: bool,
    },
    Offers,
    Accept(u32),
    Reject(u32),
    Quit,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Start {
            name,
            room,
            download_dir,
            auto_accept,
            key_file,
            ephemeral,
            listen,
            connect,
        } => {
            let key_file = if ephemeral {
                None
            } else {
                Some(key_file.unwrap_or_else(|| identity::default_key_path(&name)))
            };
            let config = libp2p_peers::PeerConfig {
                peer_name: name,
                room_name: room,
                download_dir,
                auto_accept,
                key_file,
                listen_addr: listen,
                dial_addrs: connect,
            };
            start_peer(config).await?;
        }
    }

    Ok(())
}

async fn start_peer(config: libp2p_peers::PeerConfig) -> Result<()> {
    println!(
        "{}",
        "╔══════════════════════════════════════════════════╗".bright_cyan()
    );
    println!(
        "{}",
        "║           FileMesh-RS - P2P File Transfer        ║".bright_cyan()
    );
    println!(
        "{}",
        "╚══════════════════════════════════════════════════╝".bright_cyan()
    );
    println!();

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

    let peer_handle = tokio::spawn(async move {
        if let Err(e) = libp2p_peers::run_peer(config, cmd_rx).await {
            eprintln!("{} {:#}", "Error running peer:".red(), e);
        }
    });

    tokio::spawn(async move {
        handle_user_input(cmd_tx).await;
    });

    peer_handle.await?;

    Ok(())
}

async fn handle_user_input(cmd_tx: mpsc::UnboundedSender<UserCommand>) {
    use tokio::io::{AsyncBufReadExt, BufReader};

    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();

    // Show quick usage once
    print_help();

    fn show_prompt() {
        print!("{} ", ">".bright_cyan());
        let _ = std::io::stdout().flush();
    }

    show_prompt();

    while let Ok(Some(line)) = lines.next_line().await {
        let input = line.trim();
        if input.is_empty() {
            show_prompt();
            continue;
        }

        let parts = match split_args(input) {
            Ok(p) => p,
            Err(msg) => {
                println!("{}", msg.red());
                show_prompt();
                continue;
            }
        };

        match parts.first().map(|s| s.to_ascii_lowercase()).as_deref() {
            Some("peers") => {
                let _ = cmd_tx.send(UserCommand::ListPeers);
            }
            Some("send") => {
                if let Some(cmd) = parse_send_command(&parts[1..]) {
                    let _ = cmd_tx.send(cmd);
                }
            }
            Some("offers") => {
                let _ = cmd_tx.send(UserCommand::Offers);
            }
            Some("accept") => {
                if let Some(n) = parse_offer_number(&parts[1..], "accept") {
                    let _ = cmd_tx.send(UserCommand::Accept(n));
                }
            }
            Some("reject") => {
                if let Some(n) = parse_offer_number(&parts[1..], "reject") {
                    let _ = cmd_tx.send(UserCommand::Reject(n));
                }
            }
            Some("quit") | Some("exit") => {
                let _ = cmd_tx.send(UserCommand::Quit);
                break;
            }
            Some("help") => {
                print_help();
            }
            _ => {
                println!(
                    "{} Use 'help' to see available commands",
                    "Unknown command.".red()
                );
            }
        }

        show_prompt();
    }
}

/// Split a command line into arguments. Whitespace separates arguments;
/// single or double quotes group text containing spaces (no escape sequences,
/// so Windows paths with backslashes work).
fn split_args(input: &str) -> std::result::Result<Vec<String>, String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut in_arg = false;
    let mut quote: Option<char> = None;

    for c in input.chars() {
        match quote {
            Some(q) if c == q => quote = None,
            Some(_) => current.push(c),
            None => match c {
                '"' | '\'' => {
                    quote = Some(c);
                    in_arg = true;
                }
                c if c.is_whitespace() => {
                    if in_arg {
                        args.push(std::mem::take(&mut current));
                        in_arg = false;
                    }
                }
                c => {
                    current.push(c);
                    in_arg = true;
                }
            },
        }
    }

    if quote.is_some() {
        return Err("Unclosed quote in command".to_string());
    }
    if in_arg {
        args.push(current);
    }
    Ok(args)
}

fn parse_offer_number(args: &[String], verb: &str) -> Option<u32> {
    match args {
        [n] => match n.trim_start_matches('#').parse::<u32>() {
            Ok(n) => Some(n),
            Err(_) => {
                println!("{} {} <offer number>", "Usage:".red(), verb);
                None
            }
        },
        _ => {
            println!("{} {} <offer number>", "Usage:".red(), verb);
            None
        }
    }
}

fn parse_send_command(args: &[String]) -> Option<UserCommand> {
    let mut file_path: Option<String> = None;
    let mut target: Option<String> = None;
    let mut broadcast = false;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--file" => {
                if i + 1 < args.len() {
                    file_path = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    println!("{}", "Missing file path after --file".red());
                    return None;
                }
            }
            "--to" => {
                if i + 1 < args.len() {
                    target = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    println!("{}", "Missing peer name or ID after --to".red());
                    return None;
                }
            }
            "--broadcast" => {
                broadcast = true;
                i += 1;
            }
            other => {
                println!("{} {}", "Unknown argument:".red(), other);
                return None;
            }
        }
    }

    let Some(file_path) = file_path else {
        println!("{}", "Missing required --file argument".red());
        return None;
    };

    if !broadcast && target.is_none() {
        println!(
            "{}",
            "Must specify either --to <NAME_OR_PEER_ID> or --broadcast".red()
        );
        return None;
    }

    Some(UserCommand::SendFile {
        file_path,
        target,
        broadcast,
    })
}

fn print_help() {
    println!();
    println!("{}", "Available commands:".bright_yellow());
    println!();
    println!("  {}", "peers".bright_green());
    println!("    List room members with connection types");
    println!();
    println!("  {}", "send --file <FILE> --to <NAME|PEER_ID>".bright_green());
    println!("    Offer a file to one peer (they must accept)");
    println!("    Example: send --file document.pdf --to alice");
    println!("    Quote paths containing spaces: send --file \"my notes.txt\" --to alice");
    println!();
    println!("  {}", "send --file <FILE> --broadcast".bright_green());
    println!("    Offer a file to everyone in the room");
    println!();
    println!("  {}", "offers".bright_green());
    println!("    List incoming offers waiting for your answer");
    println!();
    println!("  {}", "accept <N> / reject <N>".bright_green());
    println!("    Accept (download) or reject incoming offer number N");
    println!();
    println!("  {}", "quit / exit".bright_green());
    println!("    Exit the application");
    println!();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn split_args_handles_quotes() {
        assert_eq!(
            split_args("send --file \"my notes.txt\" --to alice").unwrap(),
            args(&["send", "--file", "my notes.txt", "--to", "alice"])
        );
        assert_eq!(
            split_args("send --file 'a b' --broadcast").unwrap(),
            args(&["send", "--file", "a b", "--broadcast"])
        );
        assert_eq!(
            split_args("  peers   ").unwrap(),
            args(&["peers"])
        );
        assert_eq!(
            split_args("send --file C:\\docs\\x.txt").unwrap(),
            args(&["send", "--file", "C:\\docs\\x.txt"])
        );
        assert!(split_args("send --file \"oops").is_err());
    }

    #[test]
    fn split_args_keeps_empty_quoted_argument() {
        assert_eq!(split_args("a \"\" b").unwrap(), args(&["a", "", "b"]));
    }

    #[test]
    fn send_command_requires_target_or_broadcast() {
        assert!(parse_send_command(&args(&["--file", "x"])).is_none());
        assert!(parse_send_command(&args(&["--to", "bob"])).is_none());
        match parse_send_command(&args(&["--file", "x", "--to", "bob"])) {
            Some(UserCommand::SendFile {
                file_path,
                target,
                broadcast,
            }) => {
                assert_eq!(file_path, "x");
                assert_eq!(target.as_deref(), Some("bob"));
                assert!(!broadcast);
            }
            other => panic!("unexpected {:?}", other),
        }
        assert!(matches!(
            parse_send_command(&args(&["--file", "x", "--broadcast"])),
            Some(UserCommand::SendFile { broadcast: true, .. })
        ));
    }

    #[test]
    fn offer_number_parsing() {
        assert_eq!(parse_offer_number(&args(&["3"]), "accept"), Some(3));
        assert_eq!(parse_offer_number(&args(&["#3"]), "accept"), Some(3));
        assert_eq!(parse_offer_number(&args(&["x"]), "accept"), None);
        assert_eq!(parse_offer_number(&args(&[]), "accept"), None);
    }
}
