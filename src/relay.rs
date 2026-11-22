//! `filemesh-rs relay`: a small public node that makes FileMesh work across the
//! internet. It does three jobs:
//!
//! 1. **Circuit relay**: peers behind NAT reserve a slot here and can then be
//!    reached through this node (and hole-punched to directly afterwards).
//! 2. **DHT bootstrap**: it runs Kademlia in server mode so peers can announce
//!    and look up room members.
//! 3. **AutoNAT server**: peers learn whether they are publicly reachable.
//!
//! It never sees file contents that peers send directly, and relayed traffic is
//! end-to-end encrypted between the peers.

use crate::identity;
use anyhow::Result;
use colored::Colorize;
use futures::StreamExt;
use libp2p::{
    autonat, identify, kad, noise, ping, relay,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux, Multiaddr, PeerId, StreamProtocol,
};
use std::{path::PathBuf, time::Duration};

pub const KAD_PROTOCOL: &str = "/filemesh/kad/1.0.0";
pub const RELAY_PROTOCOL_VERSION: &str = "/filemesh/2.0.0";

pub struct RelayConfig {
    pub listen_addrs: Vec<Multiaddr>,
    /// Public addresses to advertise to peers (e.g. `/ip4/203.0.113.7/tcp/4001`).
    pub external_addrs: Vec<Multiaddr>,
    pub key_file: Option<PathBuf>,
    pub max_circuit_secs: u64,
    /// 0 = unlimited.
    pub max_circuit_bytes: u64,
}

#[derive(NetworkBehaviour)]
struct RelayBehaviour {
    relay: relay::Behaviour,
    identify: identify::Behaviour,
    ping: ping::Behaviour,
    kademlia: kad::Behaviour<kad::store::MemoryStore>,
    autonat: autonat::Behaviour,
}

pub fn default_listen_addrs() -> Vec<Multiaddr> {
    vec![
        "/ip4/0.0.0.0/tcp/4001".parse().expect("valid multiaddr"),
        "/ip4/0.0.0.0/udp/4001/quic-v1"
            .parse()
            .expect("valid multiaddr"),
    ]
}

pub async fn run_relay(config: RelayConfig) -> Result<()> {
    let key = match &config.key_file {
        Some(path) => identity::load_or_create(path)?,
        None => libp2p::identity::Keypair::generate_ed25519(),
    };
    let local_peer_id = PeerId::from(key.public());

    let relay_config = relay::Config {
        max_circuit_duration: Duration::from_secs(config.max_circuit_secs),
        max_circuit_bytes: config.max_circuit_bytes,
        ..Default::default()
    };

    let mut swarm = libp2p::SwarmBuilder::with_existing_identity(key)
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_dns()?
        .with_behaviour(|key| {
            let peer_id = key.public().to_peer_id();
            let mut kad_cfg = kad::Config::default();
            kad_cfg.set_protocol_names(vec![StreamProtocol::new(KAD_PROTOCOL)]);
            let mut kademlia = kad::Behaviour::with_config(
                peer_id,
                kad::store::MemoryStore::new(peer_id),
                kad_cfg,
            );
            kademlia.set_mode(Some(kad::Mode::Server));

            Ok(RelayBehaviour {
                relay: relay::Behaviour::new(peer_id, relay_config),
                identify: identify::Behaviour::new(identify::Config::new(
                    RELAY_PROTOCOL_VERSION.to_string(),
                    key.public(),
                )),
                ping: ping::Behaviour::new(ping::Config::new()),
                kademlia,
                autonat: autonat::Behaviour::new(peer_id, autonat::Config::default()),
            })
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(3600)))
        .build();

    println!(
        "{} {}",
        "Relay node:".bright_cyan(),
        local_peer_id.to_string().bright_white()
    );

    for addr in &config.external_addrs {
        swarm.add_external_address(addr.clone());
    }

    let listen_addrs = if config.listen_addrs.is_empty() {
        default_listen_addrs()
    } else {
        config.listen_addrs.clone()
    };
    for addr in listen_addrs {
        swarm.listen_on(addr)?;
    }

    if config.external_addrs.is_empty() {
        println!(
            "{}",
            "No --external address given: peers will only learn this node's LAN addresses. \
             Pass --external /ip4/<PUBLIC_IP>/tcp/4001 for internet use."
                .yellow()
        );
    } else {
        println!();
        println!("{}", "Give peers one of these to use as --bootstrap:".bright_yellow());
        for addr in &config.external_addrs {
            println!("  {}/p2p/{}", addr, local_peer_id);
        }
    }
    println!();

    loop {
        match swarm.select_next_some().await {
            SwarmEvent::NewListenAddr { address, .. } => {
                println!(
                    "{} {}/p2p/{}",
                    "Listening on:".bright_cyan(),
                    address,
                    local_peer_id
                );
            }

            SwarmEvent::Behaviour(RelayBehaviourEvent::Identify(identify::Event::Received {
                peer_id,
                info,
            })) => {
                // Fill the DHT routing table with peers that speak Kademlia.
                let speaks_kad = info
                    .protocols
                    .iter()
                    .any(|p| p.as_ref() == KAD_PROTOCOL);
                if speaks_kad {
                    for addr in info.listen_addrs {
                        swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);
                    }
                }
            }

            SwarmEvent::Behaviour(RelayBehaviourEvent::Relay(event)) => match event {
                relay::Event::ReservationReqAccepted { src_peer_id, renewed } => {
                    if !renewed {
                        println!(
                            "{} {}",
                            "Reservation accepted for".bright_green(),
                            src_peer_id.to_string().bright_black()
                        );
                    }
                }
                relay::Event::CircuitReqAccepted {
                    src_peer_id,
                    dst_peer_id,
                } => println!(
                    "{} {} -> {}",
                    "Relaying circuit:".bright_green(),
                    src_peer_id.to_string().bright_black(),
                    dst_peer_id.to_string().bright_black()
                ),
                relay::Event::CircuitClosed {
                    src_peer_id,
                    dst_peer_id,
                    error,
                } => println!(
                    "{} {} -> {} {}",
                    "Circuit closed:".bright_black(),
                    src_peer_id.to_string().bright_black(),
                    dst_peer_id.to_string().bright_black(),
                    match error {
                        Some(e) => format!("({})", e),
                        None => String::new(),
                    }
                    .bright_black()
                ),
                _ => {}
            },

            _ => {}
        }
    }
}
