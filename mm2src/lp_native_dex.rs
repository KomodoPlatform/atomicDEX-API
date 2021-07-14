/******************************************************************************
 * Copyright © 2014-2019 The SuperNET Developers.                             *
 *                                                                            *
 * See the AUTHORS, DEVELOPER-AGREEMENT and LICENSE files at                  *
 * the top-level directory of this distribution for the individual copyright  *
 * holder information and the developer policies on copyright and licensing.  *
 *                                                                            *
 * Unless otherwise agreed in a custom licensing agreement, no part of the    *
 * SuperNET software, including this file may be copied, modified, propagated *
 * or distributed except according to the terms contained in the LICENSE file *
 *                                                                            *
 * Removal or modification of this copyright notice is prohibited.            *
 *                                                                            *
 ******************************************************************************/
//
//  lp_native_dex.rs
//  marketmaker
//

use coins::register_balance_update_handler;
use mm2_libp2p::{spawn_gossipsub, NetworkPorts, NodeType, RelayAddress};
use rand::rngs::SmallRng;
use rand::{random, Rng, SeedableRng};
use serde_json::{self as json};
use std::fs;
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr};
use std::path::Path;
use std::str;

#[cfg(not(target_arch = "wasm32"))]
use crate::mm2::database::init_and_migrate_db;
use crate::mm2::lp_network::{p2p_event_process_loop, P2PContext};
use crate::mm2::lp_ordermatch::{broadcast_maker_orders_keep_alive_loop, lp_ordermatch_loop, orders_kick_start,
                                BalanceUpdateOrdermatchHandler};
use crate::mm2::lp_swap::{running_swaps_num, swap_kick_starts};
use crate::mm2::rpc::spawn_rpc;
use crate::mm2::{MM_DATETIME, MM_VERSION};
use bitcrypto::sha256;
use common::executor::{spawn, spawn_boxed, Timer};
use common::log::{error, info, warn};
use common::mm_ctx::{MmArc, MmCtx};
use common::privkey::key_pair_from_seed;
use common::slurp_url;

const IP_PROVIDERS: [&str; 2] = ["http://checkip.amazonaws.com/", "http://api.ipify.org"];
const NETID_7777_SEEDNODES: [&str; 3] = ["seed1.defimania.live", "seed2.defimania.live", "seed3.defimania.live"];

#[cfg(target_arch = "wasm32")]
fn default_seednodes(netid: u16) -> Vec<RelayAddress> {
    if netid == 7777 {
        NETID_7777_SEEDNODES
            .iter()
            .map(|seed| RelayAddress::Dns(seed.to_string()))
            .collect()
    } else {
        Vec::new()
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn default_seednodes(netid: u16) -> Vec<RelayAddress> {
    if netid == 7777 {
        NETID_7777_SEEDNODES
            .iter()
            .filter_map(|seed| {
                let seed_url = format!("{}:0", *seed);
                seed_to_ipv4_string(&seed_url)
            })
            .map(RelayAddress::IPv4)
            .collect()
    } else {
        Vec::new()
    }
}

pub fn lp_ports(netid: u16) -> Result<(u16, u16, u16), String> {
    const LP_RPCPORT: u16 = 7783;
    let max_netid = (65535 - 40 - LP_RPCPORT) / 4;
    if netid > max_netid {
        return ERR!("Netid {} is larger than max {}", netid, max_netid);
    }

    let other_ports = if netid != 0 {
        let net_mod = netid % 10;
        let net_div = netid / 10;
        (net_div * 40) + LP_RPCPORT + net_mod
    } else {
        LP_RPCPORT
    };
    Ok((other_ports + 10, other_ports + 20, other_ports + 30))
}

pub fn lp_network_ports(netid: u16) -> Result<NetworkPorts, String> {
    let (_, network_port, network_ws_port) = try_s!(lp_ports(netid));
    Ok(NetworkPorts {
        tcp: network_port,
        ws: network_ws_port,
    })
}

/// Invokes `OS_ensure_directory`,
/// then prints an error and returns `false` if the directory is not writable.
fn ensure_dir_is_writable(dir_path: &Path) -> bool {
    if dir_path.exists() && !dir_path.is_dir() {
        error!("The {} is not a directory", dir_path.display());
        return false;
    } else if let Err(e) = std::fs::create_dir_all(dir_path) {
        error!("Could not create dir {}, error {}", dir_path.display(), e);
        return false;
    }
    let r: [u8; 32] = random();
    let mut check: Vec<u8> = Vec::with_capacity(r.len());
    let fname = dir_path.join("checkval");
    let mut fp = match fs::File::create(&fname) {
        Ok(fp) => fp,
        Err(_) => {
            error!("FATAL cannot create {:?}", fname);
            return false;
        },
    };
    if fp.write_all(&r).is_err() {
        error!("FATAL cannot write to {:?}", fname);
        return false;
    }
    drop(fp);
    let mut fp = match fs::File::open(&fname) {
        Ok(fp) => fp,
        Err(_) => {
            error!("FATAL cannot open {:?}", fname);
            return false;
        },
    };
    if fp.read_to_end(&mut check).is_err() || check.len() != r.len() {
        error!("FATAL cannot read {:?}", fname);
        return false;
    }
    if check != r {
        error!("FATAL expect the same {:?} data: {:?} != {:?}", fname, r, check);
        return false;
    }
    true
}

fn ensure_file_is_writable(file_path: &Path) -> Result<(), String> {
    if fs::File::open(file_path).is_err() {
        // try to create file if opening fails
        if let Err(e) = fs::OpenOptions::new().write(true).create_new(true).open(file_path) {
            return ERR!("{} when trying to create the file {}", e, file_path.display());
        }
    } else {
        // try to open file in write append mode
        if let Err(e) = fs::OpenOptions::new().write(true).append(true).open(file_path) {
            return ERR!(
                "{} when trying to open the file {} in write mode",
                e,
                file_path.display()
            );
        }
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn fix_directories(ctx: &MmCtx) -> Result<(), String> {
    let dbdir = ctx.dbdir();
    try_s!(std::fs::create_dir_all(&dbdir));

    if !ensure_dir_is_writable(&dbdir.join("SWAPS")) {
        return ERR!("SWAPS db dir is not writable");
    }
    if !ensure_dir_is_writable(&dbdir.join("SWAPS").join("MY")) {
        return ERR!("SWAPS/MY db dir is not writable");
    }
    if !ensure_dir_is_writable(&dbdir.join("SWAPS").join("STATS")) {
        return ERR!("SWAPS/STATS db dir is not writable");
    }
    if !ensure_dir_is_writable(&dbdir.join("SWAPS").join("STATS").join("MAKER")) {
        return ERR!("SWAPS/STATS/MAKER db dir is not writable");
    }
    if !ensure_dir_is_writable(&dbdir.join("SWAPS").join("STATS").join("TAKER")) {
        return ERR!("SWAPS/STATS/TAKER db dir is not writable");
    }
    if !ensure_dir_is_writable(&dbdir.join("TRANSACTIONS")) {
        return ERR!("TRANSACTIONS db dir is not writable");
    }
    if !ensure_dir_is_writable(&dbdir.join("GTC")) {
        return ERR!("GTC db dir is not writable");
    }
    if !ensure_dir_is_writable(&dbdir.join("PRICES")) {
        return ERR!("PRICES db dir is not writable");
    }
    if !ensure_dir_is_writable(&dbdir.join("UNSPENTS")) {
        return ERR!("UNSPENTS db dir is not writable");
    }
    if !ensure_dir_is_writable(&dbdir.join("ORDERS")) {
        return ERR!("ORDERS db dir is not writable");
    }
    if !ensure_dir_is_writable(&dbdir.join("ORDERS").join("MY")) {
        return ERR!("ORDERS/MY db dir is not writable");
    }
    if !ensure_dir_is_writable(&dbdir.join("ORDERS").join("MY").join("MAKER")) {
        return ERR!("ORDERS/MY/MAKER db dir is not writable");
    }
    if !ensure_dir_is_writable(&dbdir.join("ORDERS").join("MY").join("TAKER")) {
        return ERR!("ORDERS/MY/TAKER db dir is not writable");
    }
    if !ensure_dir_is_writable(&dbdir.join("ORDERS").join("MY").join("HISTORY")) {
        return ERR!("ORDERS/MY/HISTORY db dir is not writable");
    }
    if !ensure_dir_is_writable(&dbdir.join("TX_CACHE")) {
        return ERR!("TX_CACHE db dir is not writable");
    }
    try_s!(ensure_file_is_writable(&dbdir.join("GTC").join("orders")));
    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn fix_directories(ctx: &MmCtx) -> Result<(), String> {
    use std::os::raw::c_char;

    #[wasm_bindgen(raw_module = "../../../js/defined-in-js.js")]
    extern "C" {
        pub fn host_ensure_dir_is_writable(ptr: *const c_char, len: i32) -> i32;
    }
    macro_rules! writeable_dir {
        ($path: expr) => {
            let path = $path;
            let path = try_s!(path.to_str().ok_or("Non-unicode path"));
            let rc = host_ensure_dir_is_writable(path.as_ptr() as *const c_char, path.len() as i32);
            if rc != 0 {
                return ERR!("Dir '{}' not writeable: {}", path, rc);
            }
        };
    }

    let dbdir = ctx.dbdir();
    writeable_dir!(dbdir.join("SWAPS").join("MY"));
    writeable_dir!(dbdir.join("SWAPS").join("STATS").join("MAKER"));
    writeable_dir!(dbdir.join("SWAPS").join("STATS").join("TAKER"));
    writeable_dir!(dbdir.join("ORDERS").join("MY").join("MAKER"));
    writeable_dir!(dbdir.join("ORDERS").join("MY").join("TAKER"));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn migrate_db(ctx: &MmArc) -> Result<(), String> {
    let migration_num_path = ctx.dbdir().join(".migration");
    let mut current_migration = match std::fs::read(&migration_num_path) {
        Ok(bytes) => {
            let mut num_bytes = [0; 8];
            if bytes.len() == 8 {
                num_bytes.clone_from_slice(&bytes);
                u64::from_le_bytes(num_bytes)
            } else {
                0
            }
        },
        Err(_) => 0,
    };

    if current_migration < 1 {
        migration_1(ctx);
        current_migration = 1;
    }
    try_s!(std::fs::write(&migration_num_path, &current_migration.to_le_bytes()));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn migration_1(_ctx: &MmArc) {}

/// Resets the context (most of which resides currently in `lp::G` but eventually would move into `MmCtx`).
/// Restarts the peer connections.
/// Reloads the coin keys.
///
/// Besides the `passphrase` it also allows changing the `seednode` at runtime.  
/// AG: While there might be value in changing `seednode` at runtime, I'm not sure if changing `gui` is actually necessary.
///
/// AG: If possible, I think we should avoid calling this function on a working MM, using it for initialization only,
///     in order to avoid the possibility of invalid state.
/// AP: Totally agree, moreover maybe we even `must` deny calling this on a working MM as it's being refactored
pub fn lp_passphrase_init(ctx: &MmArc) -> Result<(), String> {
    let passphrase = ctx.conf["passphrase"].as_str();
    let passphrase = match passphrase {
        None | Some("") => return ERR!("jeezy says we cant use the nullstring as passphrase and I agree"),
        Some(s) => s.to_string(),
    };

    let key_pair = try_s!(key_pair_from_seed(&passphrase));
    let key_pair = try_s!(ctx.secp256k1_key_pair.pin(key_pair));
    try_s!(ctx.rmd160.pin(key_pair.public().address_hash()));
    Ok(())
}

/// Tries to serve on the given IP to check if it's available.  
/// We need this check because our external IP, particularly under NAT,
/// might be outside of the set of IPs we can open and run a server on.
///
/// Returns an error if the address did not work
/// (like when the `ip` does not belong to a connected interface).
///
/// The primary concern of this function is to test the IP,
/// but this opportunity is also used to start the HTTP fallback server,
/// in order to improve the reliability of the said server (in the Lean "stop the line" manner).
///
/// If the IP has passed the communication check then a shutdown Sender is returned.
/// Dropping or using that Sender will stop the HTTP fallback server.
///
/// Also the port of the HTTP fallback server is returned.
#[cfg(not(target_arch = "wasm32"))]
fn test_ip(ctx: &MmArc, ip: IpAddr) -> Result<(), String> {
    let netid = ctx.netid();

    // Try a few pseudo-random ports.
    // `netid` is used as the seed in order for the port selection to be determenistic,
    // similar to how the port selection and probing worked before (since MM1)
    // and in order to reduce the likehood of *unexpected* port conflicts.
    let mut attempts_left = 9;
    let mut rng = SmallRng::seed_from_u64(netid as u64);
    loop {
        if attempts_left < 1 {
            break ERR!("Out of attempts");
        }
        attempts_left -= 1;
        // TODO: Avoid `mypubport`.
        let port = rng.gen_range(1111, 65535);
        info!("Trying to bind on {}:{}", ip, port);
        match std::net::TcpListener::bind((ip, port)) {
            Ok(_) => break Ok(()),
            Err(err) => {
                if attempts_left == 0 {
                    break ERR!("{}", err);
                }
                continue;
            },
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn seed_to_ipv4_string(seed: &str) -> Option<String> {
    use std::net::ToSocketAddrs;
    match seed.to_socket_addrs() {
        Ok(mut iter) => match iter.next() {
            Some(addr) => {
                if addr.is_ipv4() {
                    Some(addr.ip().to_string())
                } else {
                    warn!("Seed {} resolved to IPv6 {} which is not supported", seed, addr);
                    None
                }
            },
            None => {
                warn!("Seed {} to_socket_addrs empty iter", seed);
                None
            },
        },
        Err(e) => {
            error!("Couldn't resolve '{}' seed: {}", seed, e);
            None
        },
    }
}

#[cfg_attr(target_arch = "wasm32", allow(unused_variables))]
/// * `ctx_cb` - callback used to share the `MmCtx` ID with the call site.
pub async fn lp_init(ctx: MmArc) -> Result<(), String> {
    info!("Version: {} DT {}", MM_VERSION, MM_DATETIME);
    try_s!(lp_passphrase_init(&ctx));

    try_s!(fix_directories(&ctx));
    #[cfg(not(target_arch = "wasm32"))]
    {
        try_s!(ctx.init_sqlite_connection());
        try_s!(init_and_migrate_db(&ctx, &ctx.sqlite_connection()));
        try_s!(migrate_db(&ctx));
    }

    try_s!(init_p2p(ctx.clone()).await);

    let balance_update_ordermatch_handler = BalanceUpdateOrdermatchHandler::new(ctx.clone());
    register_balance_update_handler(ctx.clone(), Box::new(balance_update_ordermatch_handler)).await;

    try_s!(ctx.initialized.pin(true));

    #[cfg(not(target_arch = "wasm32"))]
    {
        // launch kickstart threads before RPC is available, this will prevent the API user to place
        // an order and start new swap that might get started 2 times because of kick-start
        let mut coins_needed_for_kick_start = swap_kick_starts(ctx.clone());
        coins_needed_for_kick_start.extend(try_s!(orders_kick_start(&ctx).await));
        *(try_s!(ctx.coins_needed_for_kick_start.lock())) = coins_needed_for_kick_start;
    }

    spawn(lp_ordermatch_loop(ctx.clone()));

    spawn(broadcast_maker_orders_keep_alive_loop(ctx.clone()));

    let ctx_id = try_s!(ctx.ffi_handle());

    spawn_rpc(ctx_id);
    let ctx_c = ctx.clone();
    spawn(async move {
        if let Err(err) = ctx_c.init_metrics() {
            warn!("Couldn't initialize metrics system: {}", err);
        }
    });
    // In the mobile version we might depend on `lp_init` staying around until the context stops.
    loop {
        if ctx.is_stopping() {
            break;
        };
        Timer::sleep(0.2).await
    }

    // wait for swaps to stop
    loop {
        if running_swaps_num(&ctx) == 0 {
            break;
        };
        Timer::sleep(0.2).await
    }
    Ok(())
}

fn simple_ip_extractor(ip: &str) -> Result<IpAddr, String> {
    let ip = ip.trim();
    Ok(match ip.parse() {
        Ok(ip) => ip,
        Err(err) => return ERR!("Error parsing IP address '{}': {}", ip, err),
    })
}

/// Detect the real IP address.
///
/// We're detecting the outer IP address, visible to the internet.
/// Later we'll try to *bind* on this IP address,
/// and this will break under NAT or forwarding because the internal IP address will be different.
/// Which might be a good thing, allowing us to detect the likehoodness of NAT early.
#[cfg(not(target_arch = "wasm32"))]
async fn detect_myipaddr(ctx: MmArc) -> Result<IpAddr, String> {
    for url in IP_PROVIDERS.iter() {
        info!("Trying to fetch the real IP from '{}' ...", url);
        let (status, _headers, ip) = match slurp_url(url).await {
            Ok(t) => t,
            Err(err) => {
                error!("Failed to fetch IP from '{}': {}", url, err);
                continue;
            },
        };
        if !status.is_success() {
            error!("Failed to fetch IP from '{}': status {:?}", url, status);
            continue;
        }
        let ip = match std::str::from_utf8(&ip) {
            Ok(ip) => ip,
            Err(err) => {
                error!("Failed to fetch IP from '{}', not UTF-8: {}", url, err);
                continue;
            },
        };
        let ip = match simple_ip_extractor(ip) {
            Ok(ip) => ip,
            Err(err) => {
                error!("Failed to parse IP '{}' fetched from '{}': {}", ip, url, err);
                continue;
            },
        };

        // Try to bind on this IP.
        // If we're not behind a NAT then the bind will likely succeed.
        // If the bind fails then emit a user-visible warning and fall back to 0.0.0.0.
        match test_ip(&ctx, ip) {
            Ok(_) => {
                ctx.log.log(
                    "🙂",
                    &[&"myipaddr"],
                    &fomat! (
                        "We've detected an external IP " (ip) " and we can bind on it"
                        ", so probably a dedicated IP."),
                );
                return Ok(ip);
            },
            Err(err) => error!("IP {} not available: {}", ip, err),
        }
        let all_interfaces = Ipv4Addr::new(0, 0, 0, 0).into();
        if test_ip(&ctx, all_interfaces).is_ok() {
            ctx.log.log ("😅", &[&"myipaddr"], &fomat! (
                    "We couldn't bind on the external IP " (ip) ", so NAT is likely to be present. We'll be okay though."));
            return Ok(all_interfaces);
        }
        let localhost = Ipv4Addr::new(127, 0, 0, 1).into();
        if test_ip(&ctx, localhost).is_ok() {
            ctx.log.log(
                "🤫",
                &[&"myipaddr"],
                &fomat! (
                    "We couldn't bind on " (ip) " or 0.0.0.0!"
                    " Looks like we can bind on 127.0.0.1 as a workaround, but that's not how we're supposed to work."),
            );
            return Ok(localhost);
        }
        ctx.log.log(
            "🤒",
            &[&"myipaddr"],
            &fomat! (
                "Couldn't bind on " (ip) ", 0.0.0.0 or 127.0.0.1."),
        );
        return Ok(all_interfaces); // Seems like a better default than 127.0.0.1, might still work for other ports.
    }
    ERR!("Couldn't fetch the real IP")
}

#[cfg(not(target_arch = "wasm32"))]
async fn myipaddr(ctx: MmArc) -> Result<IpAddr, String> {
    let myipaddr: IpAddr = if Path::new("myipaddr").exists() {
        match fs::File::open("myipaddr") {
            Ok(mut f) => {
                let mut buf = String::new();
                if let Err(err) = f.read_to_string(&mut buf) {
                    return ERR!("Can't read from 'myipaddr': {}", err);
                }
                try_s!(simple_ip_extractor(&buf))
            },
            Err(err) => return ERR!("Can't read from 'myipaddr': {}", err),
        }
    } else if !ctx.conf["myipaddr"].is_null() {
        let s = try_s!(ctx.conf["myipaddr"].as_str().ok_or("'myipaddr' is not a string"));
        try_s!(simple_ip_extractor(s))
    } else {
        try_s!(detect_myipaddr(ctx).await)
    };
    Ok(myipaddr)
}

async fn init_p2p(ctx: MmArc) -> Result<(), String> {
    let i_am_seed = ctx.conf["i_am_seed"].as_bool().unwrap_or(false);
    let netid = ctx.netid();

    let seednodes = try_s!(seednodes(&ctx));

    let ctx_on_poll = ctx.clone();
    let force_p2p_key = if i_am_seed {
        let key = sha256(&*ctx.secp256k1_key_pair().private().secret);
        Some(key.take())
    } else {
        None
    };

    let node_type = if i_am_seed {
        try_s!(relay_node_type(&ctx).await)
    } else {
        try_s!(light_node_type(&ctx))
    };

    let spawn_result = spawn_gossipsub(netid, force_p2p_key, spawn_boxed, seednodes, node_type, move |swarm| {
        let behaviour = swarm.behaviour();
        mm_gauge!(
            ctx_on_poll.metrics,
            "p2p.connected_relays.len",
            behaviour.connected_relays_len() as i64
        );
        mm_gauge!(
            ctx_on_poll.metrics,
            "p2p.relay_mesh.len",
            behaviour.relay_mesh_len() as i64
        );
        let (period, received_msgs) = behaviour.received_messages_in_period();
        mm_gauge!(
            ctx_on_poll.metrics,
            "p2p.received_messages.period_in_secs",
            period.as_secs() as i64
        );

        mm_gauge!(ctx_on_poll.metrics, "p2p.received_messages.count", received_msgs as i64);

        let connected_peers_count = behaviour.connected_peers_len();

        mm_gauge!(
            ctx_on_poll.metrics,
            "p2p.connected_peers.count",
            connected_peers_count as i64
        );
    })
    .await;
    let (cmd_tx, event_rx, peer_id, p2p_abort) = try_s!(spawn_result);
    let mut p2p_abort = Some(p2p_abort);
    ctx.on_stop(Box::new(move || {
        if let Some(handle) = p2p_abort.take() {
            drop(handle);
        }
        Ok(())
    }));
    try_s!(ctx.peer_id.pin(peer_id.to_string()));
    let p2p_context = P2PContext::new(cmd_tx);
    p2p_context.store_to_mm_arc(&ctx);
    spawn(p2p_event_process_loop(ctx.clone(), event_rx, i_am_seed));

    Ok(())
}

fn seednodes(ctx: &MmArc) -> Result<Vec<RelayAddress>, String> {
    if ctx.conf["seednodes"].is_null() {
        if ctx.p2p_in_memory() {
            // If the network is in memory, there is no need to use default seednodes.
            return Ok(Vec::new());
        }
        return Ok(default_seednodes(ctx.netid()));
    }

    json::from_value(ctx.conf["seednodes"].clone()).map_err(|e| ERRL!("{}", e))
}

#[cfg(target_arch = "wasm32")]
async fn relay_node_type(ctx: &MmArc) -> Result<NodeType, String> {
    if ctx.p2p_in_memory() {
        return relay_in_memory_node_type(ctx);
    }
    ERR!("WASM node can be a seed if only 'p2p_in_memory' is true")
}

#[cfg(not(target_arch = "wasm32"))]
async fn relay_node_type(ctx: &MmArc) -> Result<NodeType, String> {
    if ctx.p2p_in_memory() {
        return relay_in_memory_node_type(ctx);
    }

    let netid = ctx.netid();
    let ip = try_s!(myipaddr(ctx.clone()).await);
    let network_ports = try_s!(lp_network_ports(netid));
    Ok(NodeType::Relay { ip, network_ports })
}

fn relay_in_memory_node_type(ctx: &MmArc) -> Result<NodeType, String> {
    Ok(NodeType::RelayInMemory {
        port: ctx
            .p2p_in_memory_port()
            .ok_or_else(|| ERRL!("'p2p_in_memory_port' not found in the config"))?,
    })
}

fn light_node_type(ctx: &MmArc) -> Result<NodeType, String> {
    if ctx.p2p_in_memory() {
        return Ok(NodeType::LightInMemory);
    }

    let netid = ctx.netid();
    let network_ports = try_s!(lp_network_ports(netid));
    Ok(NodeType::Light { network_ports })
}
