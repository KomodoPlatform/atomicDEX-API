use super::{lp_main, LpMainParams};
use bigdecimal::BigDecimal;
use common::executor::Timer;
use common::for_tests::{check_my_swap_status, check_recent_swaps, check_stats_swap_status,
                        enable_native as enable_native_impl, enable_qrc20, find_metrics_in_json, from_env_file,
                        mm_spat, new_mm2_temp_folder_path, LocalStart, MarketMakerIt, RaiiDump, MAKER_ERROR_EVENTS,
                        MAKER_SUCCESS_EVENTS, TAKER_ERROR_EVENTS, TAKER_SUCCESS_EVENTS};
use common::mm_metrics::{MetricType, MetricsJson};
use common::mm_number::{Fraction, MmNumber};
use common::privkey::key_pair_from_seed;
use common::{block_on, slurp};
use http::StatusCode;
use num_rational::BigRational;
use serde_json::{self as json, Value as Json};
use std::collections::HashMap;
use std::convert::identity;
use std::env::{self, var};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::thread;
use std::time::Duration;
use uuid::Uuid;

cfg_native! {
    use hyper::header::ACCESS_CONTROL_ALLOW_ORIGIN;
    use common::for_tests::get_passphrase;
}

cfg_wasm32! {
    use common::call_back;
    use wasm_bindgen::prelude::*;
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);
}

#[path = "mm2_tests/electrums.rs"] pub mod electrums;
use electrums::*;

#[path = "mm2_tests/structs.rs"] pub mod structs;
use structs::*;

// TODO: Consider and/or try moving the integration tests into separate Rust files.
// "Tests in your src files should be unit tests, and tests in tests/ should be integration-style tests."
// - https://doc.rust-lang.org/cargo/guide/tests.html

/// Ideally, this function should be replaced everywhere with `enable_electrum_json`.
async fn enable_electrum(mm: &MarketMakerIt, coin: &str, tx_history: bool, urls: &[&str]) -> EnableElectrumResponse {
    use common::for_tests::enable_electrum as enable_electrum_impl;

    let value = enable_electrum_impl(mm, coin, tx_history, urls).await;
    json::from_value(value).unwrap()
}

async fn enable_electrum_json(
    mm: &MarketMakerIt,
    coin: &str,
    tx_history: bool,
    servers: Vec<Json>,
) -> EnableElectrumResponse {
    use common::for_tests::enable_electrum_json as enable_electrum_impl;

    let value = enable_electrum_impl(mm, coin, tx_history, servers).await;
    json::from_value(value).unwrap()
}

async fn enable_native(mm: &MarketMakerIt, coin: &str, urls: &[&str]) -> EnableElectrumResponse {
    let value = enable_native_impl(mm, coin, urls).await;
    json::from_value(value).unwrap()
}

async fn enable_coins_eth_electrum(
    mm: &MarketMakerIt,
    eth_urls: &[&str],
) -> HashMap<&'static str, EnableElectrumResponse> {
    let mut replies = HashMap::new();
    replies.insert("RICK", enable_electrum_json(mm, "RICK", false, rick_electrums()).await);
    replies.insert(
        "MORTY",
        enable_electrum_json(mm, "MORTY", false, morty_electrums()).await,
    );
    replies.insert("ETH", enable_native(mm, "ETH", eth_urls).await);
    replies.insert("JST", enable_native(mm, "JST", eth_urls).await);
    #[cfg(feature = "zhtlc")]
    replies.insert("ZOMBIE", enable_native(mm, "ZOMBIE", eth_urls).await);
    replies
}

fn addr_from_enable<'a>(enable_response: &'a HashMap<&str, EnableElectrumResponse>, coin: &str) -> &'a str {
    &enable_response.get(coin).unwrap().address
}

fn rmd160_from_passphrase(passphrase: &str) -> [u8; 20] {
    key_pair_from_seed(passphrase).unwrap().public().address_hash().take()
}

/*
portfolio is removed from dependencies temporary
#[test]
#[ignore]
fn test_autoprice_coingecko() {portfolio::portfolio_tests::test_autoprice_coingecko (local_start())}

#[test]
#[ignore]
fn test_autoprice_coinmarketcap() {portfolio::portfolio_tests::test_autoprice_coinmarketcap (local_start())}

#[test]
fn test_fundvalue() {portfolio::portfolio_tests::test_fundvalue (local_start())}
*/

/// Integration test for RPC server.
/// Check that MM doesn't crash in case of invalid RPC requests
#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_rpc() {
    let (_, mm, _dump_log, _dump_dashboard) = mm_spat(local_start(), &identity);

    let no_method = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "coin": "RICK",
        "ipaddr": "electrum1.cipig.net",
        "port": 10017
    })))
    .unwrap();
    assert!(no_method.0.is_server_error());
    assert_eq!((no_method.2)[ACCESS_CONTROL_ALLOW_ORIGIN], "http://localhost:4000");

    let not_json = mm.rpc_str("It's just a string").unwrap();
    assert!(not_json.0.is_server_error());
    assert_eq!((not_json.2)[ACCESS_CONTROL_ALLOW_ORIGIN], "http://localhost:4000");

    let unknown_method = block_on(mm.rpc(json! ({
        "method": "unknown_method",
    })))
    .unwrap();

    assert!(unknown_method.0.is_server_error());
    assert_eq!((unknown_method.2)[ACCESS_CONTROL_ALLOW_ORIGIN], "http://localhost:4000");

    let version = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "version",
    })))
    .unwrap();
    assert_eq!(version.0, StatusCode::OK);
    assert_eq!((version.2)[ACCESS_CONTROL_ALLOW_ORIGIN], "http://localhost:4000");

    let help = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "help",
    })))
    .unwrap();
    assert_eq!(help.0, StatusCode::OK);
    assert_eq!((help.2)[ACCESS_CONTROL_ALLOW_ORIGIN], "http://localhost:4000");

    block_on(mm.stop()).unwrap();
    // unwrap! (mm.wait_for_log (9., &|log| log.contains ("on_stop] firing shutdown_tx!")));
    // TODO (workaround libtorrent hanging in delete) // unwrap! (mm.wait_for_log (9., &|log| log.contains ("LogState] Bye!")));
}

/// This is not a separate test but a helper used by `MarketMakerIt` to run the MarketMaker from the test binary.
#[test]
fn test_mm_start() {
    if let Ok(conf) = var("_MM2_TEST_CONF") {
        log!("test_mm_start] Starting the MarketMaker...");
        let conf: Json = json::from_str(&conf).unwrap();
        let params = LpMainParams::with_conf(conf);
        block_on(lp_main(params, &|_ctx| ())).unwrap()
    }
}

#[allow(unused_variables)]
fn chdir(dir: &Path) {
    #[cfg(not(target_arch = "wasm32"))]
    {
        #[cfg(not(windows))]
        {
            use std::ffi::CString;
            let dir_s = dir.to_str().unwrap();
            let dir_c = CString::new(dir_s).unwrap();
            let rc = unsafe { libc::chdir(dir_c.as_ptr()) };
            assert_eq!(rc, 0, "Can not chdir to {:?}", dir);
        }

        #[cfg(windows)]
        {
            use std::ffi::CString;
            use winapi::um::processenv::SetCurrentDirectoryA;
            let dir = dir.to_str().unwrap();
            let dir = CString::new(dir).unwrap();
            // https://docs.microsoft.com/en-us/windows/desktop/api/WinBase/nf-winbase-setcurrentdirectory
            let rc = unsafe { SetCurrentDirectoryA(dir.as_ptr()) };
            assert_ne!(rc, 0);
        }
    }
}

/// Typically used when the `LOCAL_THREAD_MM` env is set, helping debug the tested MM.  
/// NB: Accessing `lp_main` this function have to reside in the mm2 binary crate. We pass a pointer to it to subcrates.
#[cfg(not(target_arch = "wasm32"))]
fn local_start_impl(folder: PathBuf, log_path: PathBuf, mut conf: Json) {
    thread::Builder::new()
        .name("MM".into())
        .spawn(move || {
            if conf["log"].is_null() {
                conf["log"] = log_path.to_str().unwrap().into();
            } else {
                let path = Path::new(conf["log"].as_str().expect("log is not a string"));
                assert_eq!(log_path, path);
            }

            log! ({"local_start] MM in a thread, log {:?}.", log_path});

            chdir(&folder);

            let params = LpMainParams::with_conf(conf);
            block_on(lp_main(params, &|_ctx| ())).unwrap()
        })
        .unwrap();
}

/// Starts the WASM version of MM.
#[cfg(target_arch = "wasm32")]
fn wasm_start_impl(ctx: crate::common::mm_ctx::MmArc) {
    common::executor::spawn(async move {
        super::lp_init(ctx).await.unwrap();
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn local_start() -> LocalStart { local_start_impl }

#[cfg(target_arch = "wasm32")]
fn local_start() -> LocalStart { wasm_start_impl }

#[cfg(not(target_arch = "wasm32"))]
macro_rules! local_start {
    ($who: expr) => {
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == $who => Some(local_start()),
            _ => None,
        }
    };
}

#[cfg(target_arch = "wasm32")]
macro_rules! local_start {
    ($who: expr) => {
        Some(local_start())
    };
}

/// https://github.com/artemii235/SuperNET/issues/241
#[test]
#[cfg(not(target_arch = "wasm32"))]
fn alice_can_see_the_active_order_after_connection() {
    let coins = json!([
        {"coin":"RICK","asset":"RICK","rpcport":8923,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","rpcport":11608,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"},"rpcport":80},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20", "protocol_data":{"platform":"ETH","contract_address":"0xc0eb7AeD740E1796992A08962c15661bDEB58003"}}}
    ]);

    // start bob and immediately place the order
    let mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": "bob passphrase",
            "coins": coins,
            "rpc_password": "pass",
            "i_am_seed": true,
        }),
        "pass".into(),
        local_start!("bob"),
    )
    .unwrap();
    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log!({"Bob log path: {}", mm_bob.log_path.display()});
    // Enable coins on Bob side. Print the replies in case we need the "address".
    log!({ "enable_coins (bob): {:?}", block_on(enable_coins_eth_electrum(&mm_bob, &["https://ropsten.infura.io/v3/c01c1b4cf66642528547624e1d6d9d6b"])) });
    // issue sell request on Bob side by setting base/rel price
    log!("Issue bob sell request");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "setprice",
        "base": "RICK",
        "rel": "MORTY",
        "price": 0.9,
        "volume": "0.9",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!setprice: {}", rc.1);
    // Bob orderbook must show the new order
    log!("Get RICK/MORTY orderbook on Bob side");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "orderbook",
        "base": "RICK",
        "rel": "MORTY",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!orderbook: {}", rc.1);

    let bob_orderbook: OrderbookResponse = json::from_str(&rc.1).unwrap();
    log!("Bob orderbook "[bob_orderbook]);
    assert!(bob_orderbook.asks.len() > 0, "Bob RICK/MORTY asks are empty");
    assert_eq!(BigDecimal::from_str("0.9").unwrap(), bob_orderbook.asks[0].max_volume);

    // start eve and immediately place the order
    let mm_eve = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": "eve passphrase",
            "coins": coins,
            "rpc_password": "pass",
            "seednodes": [fomat!((mm_bob.ip))],
        }),
        "pass".into(),
        local_start!("bob"),
    )
    .unwrap();
    let (_eve_dump_log, _eve_dump_dashboard) = mm_eve.mm_dump();
    log!({ "Eve log path: {}", mm_eve.log_path.display() });
    // Enable coins on Eve side. Print the replies in case we need the "address".
    log!({ "enable_coins (eve): {:?}", block_on(enable_coins_eth_electrum(&mm_eve, &["https://ropsten.infura.io/v3/c01c1b4cf66642528547624e1d6d9d6b"])) });
    // issue sell request on Eve side by setting base/rel price
    log!("Issue eve sell request");
    let rc = block_on(mm_eve.rpc(json! ({
        "userpass": mm_eve.userpass,
        "method": "setprice",
        "base": "RICK",
        "rel": "MORTY",
        "price": "1",
        "volume": "0.9",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!setprice: {}", rc.1);
    // issue sell request on Eve side by setting base/rel price
    log!("Issue eve sell request");
    let rc = block_on(mm_eve.rpc(json! ({
        "userpass": mm_eve.userpass,
        "method": "setprice",
        "base": "MORTY",
        "rel": "RICK",
        "price": "1",
        "volume": "0.9",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!setprice: {}", rc.1);

    log!("Get RICK/MORTY orderbook on Eve side");
    let rc = block_on(mm_eve.rpc(json! ({
        "userpass": mm_eve.userpass,
        "method": "orderbook",
        "base": "RICK",
        "rel": "MORTY",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!orderbook: {}", rc.1);

    let eve_orderbook: OrderbookResponse = json::from_str(&rc.1).unwrap();
    log!("Eve orderbook "[eve_orderbook]);
    assert_eq!(
        eve_orderbook.asks.len(),
        2,
        "Eve RICK/MORTY orderbook must have exactly 2 asks"
    );
    assert_eq!(
        eve_orderbook.bids.len(),
        1,
        "Eve RICK/MORTY orderbook must have exactly 1 bid"
    );

    log!("Give Bob 2 seconds to import Eve order");
    thread::sleep(Duration::from_secs(2));
    log!("Get RICK/MORTY orderbook on Bob side");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "orderbook",
        "base": "RICK",
        "rel": "MORTY",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!orderbook: {}", rc.1);

    let bob_orderbook: OrderbookResponse = json::from_str(&rc.1).unwrap();
    log!("Bob orderbook "[bob_orderbook]);
    assert_eq!(
        bob_orderbook.asks.len(),
        2,
        "Bob RICK/MORTY orderbook must have exactly 2 asks"
    );
    assert_eq!(
        bob_orderbook.bids.len(),
        1,
        "Bob RICK/MORTY orderbook must have exactly 1 bid"
    );

    let mm_alice = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("ALICE_TRADE_IP") .ok(),
            "rpcip": env::var ("ALICE_TRADE_IP") .ok(),
            "passphrase": "alice passphrase",
            "coins": coins,
            "seednodes": [fomat!((mm_bob.ip))],
            "rpc_password": "pass",
        }),
        "pass".into(),
        local_start!("alice"),
    )
    .unwrap();

    let (_alice_dump_log, _alice_dump_dashboard) = mm_alice.mm_dump();
    log!({ "Alice log path: {}", mm_alice.log_path.display() });

    // Enable coins on Alice side. Print the replies in case we need the "address".
    log!({ "enable_coins (alice): {:?}", block_on(enable_coins_eth_electrum(&mm_alice, &["https://ropsten.infura.io/v3/c01c1b4cf66642528547624e1d6d9d6b"])) });

    log!("Get RICK/MORTY orderbook on Alice side");
    let rc = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "method": "orderbook",
        "base": "RICK",
        "rel": "MORTY",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!orderbook: {}", rc.1);

    let alice_orderbook: OrderbookResponse = json::from_str(&rc.1).unwrap();
    log!("Alice orderbook "[alice_orderbook]);
    assert_eq!(
        alice_orderbook.asks.len(),
        2,
        "Alice RICK/MORTY orderbook must have exactly 2 asks"
    );
    assert_eq!(
        alice_orderbook.bids.len(),
        1,
        "Alice RICK/MORTY orderbook must have exactly 1 bid"
    );

    block_on(mm_bob.stop()).unwrap();
    block_on(mm_alice.stop()).unwrap();
    block_on(mm_eve.stop()).unwrap();
}

/// https://github.com/KomodoPlatform/atomicDEX-API/issues/886#issuecomment-812489844
#[test]
#[cfg(not(target_arch = "wasm32"))]
fn orders_of_banned_pubkeys_should_not_be_displayed() {
    let coins = json!([
        {"coin":"RICK","asset":"RICK","rpcport":8923,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","rpcport":11608,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"},"rpcport":80},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20", "protocol_data":{"platform":"ETH","contract_address":"0xc0eb7AeD740E1796992A08962c15661bDEB58003"}}}
    ]);

    // start bob and immediately place the order
    let mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": "bob passphrase",
            "coins": coins,
            "rpc_password": "pass",
            "i_am_seed": true,
        }),
        "pass".into(),
        local_start!("bob"),
    )
    .unwrap();
    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log!({"Bob log path: {}", mm_bob.log_path.display()});
    // Enable coins on Bob side. Print the replies in case we need the "address".
    log!({ "enable_coins (bob): {:?}", block_on(enable_coins_eth_electrum(&mm_bob, &["https://ropsten.infura.io/v3/c01c1b4cf66642528547624e1d6d9d6b"])) });
    // issue sell request on Bob side by setting base/rel price
    log!("Issue bob sell request");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "setprice",
        "base": "RICK",
        "rel": "MORTY",
        "price": 0.9,
        "volume": "0.9",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!setprice: {}", rc.1);

    let mut mm_alice = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("ALICE_TRADE_IP") .ok(),
            "rpcip": env::var ("ALICE_TRADE_IP") .ok(),
            "passphrase": "alice passphrase",
            "coins": coins,
            "seednodes": [fomat!((mm_bob.ip))],
            "rpc_password": "pass",
        }),
        "pass".into(),
        local_start!("alice"),
    )
    .unwrap();

    let (_alice_dump_log, _alice_dump_dashboard) = mm_alice.mm_dump();
    log!({ "Alice log path: {}", mm_alice.log_path.display() });

    log!("Ban Bob pubkey on Alice side");
    let rc = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "method": "ban_pubkey",
        "pubkey": "2cd3021a2197361fb70b862c412bc8e44cff6951fa1de45ceabfdd9b4c520420",
        "reason": "test",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!ban_pubkey: {}", rc.1);

    log!("Get RICK/MORTY orderbook on Alice side");
    let rc = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "method": "orderbook",
        "base": "RICK",
        "rel": "MORTY",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!orderbook: {}", rc.1);

    let alice_orderbook: OrderbookResponse = json::from_str(&rc.1).unwrap();
    log!("Alice orderbook "[alice_orderbook]);
    assert_eq!(
        alice_orderbook.asks.len(),
        0,
        "Alice RICK/MORTY orderbook must have no asks"
    );

    block_on(mm_alice.wait_for_log(22., |log| {
        log.contains("Pubkey 022cd3021a2197361fb70b862c412bc8e44cff6951fa1de45ceabfdd9b4c520420 is banned")
    }))
    .unwrap();

    block_on(mm_bob.stop()).unwrap();
    block_on(mm_alice.stop()).unwrap();
}

#[test]
fn log_test_status() { common::log::tests::test_status() }

#[test]
fn log_test_printed_dashboard() { common::log::tests::test_printed_dashboard() }

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_my_balance() {
    let coins = json!([
        {"coin":"RICK","asset":"RICK","rpcport":8923,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
    ]);

    let mm = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "passphrase": "bob passphrase",
            "coins": coins,
            "i_am_seed": true,
            "rpc_password": "pass",
        }),
        "pass".into(),
        local_start!("bob"),
    )
    .unwrap();
    let (_dump_log, _dump_dashboard) = mm.mm_dump();
    log!({"log path: {}", mm.log_path.display()});
    // Enable RICK.
    let json = block_on(enable_electrum(&mm, "RICK", false, &[
        "electrum1.cipig.net:10017",
        "electrum2.cipig.net:10017",
        "electrum3.cipig.net:10017",
    ]));
    assert_eq!(json.balance, "7.777".parse().unwrap());

    let my_balance = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "my_balance",
        "coin": "RICK",
    })))
    .unwrap();
    assert_eq!(
        my_balance.0,
        StatusCode::OK,
        "RPC «my_balance» failed with status «{}»",
        my_balance.0
    );
    let json: Json = json::from_str(&my_balance.1).unwrap();
    let my_balance = json["balance"].as_str().unwrap();
    assert_eq!(my_balance, "7.777");
    let my_unspendable_balance = json["unspendable_balance"].as_str().unwrap();
    assert_eq!(my_unspendable_balance, "0");
    let my_address = json["address"].as_str().unwrap();
    assert_eq!(my_address, "RRnMcSeKiLrNdbp91qNVQwwXx5azD4S4CD");
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_p2wpkh_my_balance() {
    let seed = "valley embody about obey never adapt gesture trust screen tube glide bread";

    let coins = json! ([
        {
            "coin": "tBTC",
            "name": "tbitcoin",
            "fname": "tBitcoin",
            "rpcport": 18332,
            "pubtype": 111,
            "p2shtype": 196,
            "wiftype": 239,
            "segwit": true,
            "bech32_hrp": "tb",
            "txfee": 0,
            "estimate_fee_mode": "ECONOMICAL",
            "mm2": 1,
            "required_confirmations": 0,
            "protocol": {
                "type": "UTXO"
            }
        }
    ]);

    let mm = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "passphrase": seed.to_string(),
            "coins": coins,
            "i_am_seed": true,
            "rpc_password": "pass",
        }),
        "pass".into(),
        local_start!("bob"),
    )
    .unwrap();
    let (_dump_log, _dump_dashboard) = mm.mm_dump();
    log!({ "log path: {}", mm.log_path.display() });

    let electrum = block_on(mm.rpc(json!({
        "userpass": mm.userpass,
        "method": "electrum",
        "coin": "tBTC",
        "servers": [{"url":"electrum1.cipig.net:10068"},{"url":"electrum2.cipig.net:10068"},{"url":"electrum3.cipig.net:10068"}],
        "mm2": 1,
        "address_format": {
            "format": "segwit",
        },
    }))).unwrap();
    assert_eq!(
        electrum.0,
        StatusCode::OK,
        "RPC «electrum» failed with {} {}",
        electrum.0,
        electrum.1
    );

    let my_balance = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "my_balance",
        "coin": "tBTC",
    })))
    .unwrap();
    let json: Json = json::from_str(&my_balance.1).unwrap();
    let my_balance = json["balance"].as_str().unwrap();
    assert_eq!(my_balance, "0.002");
    let my_unspendable_balance = json["unspendable_balance"].as_str().unwrap();
    assert_eq!(my_unspendable_balance, "0");
    let my_address = json["address"].as_str().unwrap();
    assert_eq!(my_address, "tb1qssfmay8nnghx7ynlznejnjxn6m4pemz9v7fsxy");
}

fn check_set_price_fails(mm: &MarketMakerIt, base: &str, rel: &str) {
    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "setprice",
        "base": base,
        "rel": rel,
        "price": 0.9,
        "volume": 1,
    })))
    .unwrap();
    assert!(
        rc.0.is_server_error(),
        "!setprice success but should be error: {}",
        rc.1
    );
}

fn check_buy_fails(mm: &MarketMakerIt, base: &str, rel: &str, vol: f64) {
    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "buy",
        "base": base,
        "rel": rel,
        "volume": vol,
        "price": 0.9
    })))
    .unwrap();
    assert!(rc.0.is_server_error(), "!buy success but should be error: {}", rc.1);
}

fn check_sell_fails(mm: &MarketMakerIt, base: &str, rel: &str, vol: f64) {
    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "sell",
        "base": base,
        "rel": rel,
        "volume": vol,
        "price": 0.9
    })))
    .unwrap();
    assert!(rc.0.is_server_error(), "!sell success but should be error: {}", rc.1);
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_check_balance_on_order_post() {
    let coins = json!([
        {"coin":"RICK","asset":"RICK","rpcport":8923,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","rpcport":11608,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"},"rpcport":80},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20", "protocol_data":{"platform":"ETH","contract_address":"0x996a8aE0304680F6A69b8A9d7C6E37D65AB5AB56"}}}
    ]);

    // start bob and immediately place the order
    let mm = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": "bob passphrase check balance on order post",
            "coins": coins,
            "i_am_seed": true,
            "rpc_password": "pass",
        }),
        "pass".into(),
        local_start!("bob"),
    )
    .unwrap();
    let (_dump_log, _dump_dashboard) = mm.mm_dump();
    log!({"Log path: {}", mm.log_path.display()});
    // Enable coins. Print the replies in case we need the "address".
    log! ({"enable_coins (bob): {:?}", block_on (enable_coins_eth_electrum (&mm, &["http://eth1.cipig.net:8555"]))});
    // issue sell request by setting base/rel price

    // Expect error as MORTY balance is 0
    check_set_price_fails(&mm, "MORTY", "RICK");
    // Address has enough RICK, but doesn't have ETH, so setprice call should fail because maker will not have gas to spend ETH taker payment.
    check_set_price_fails(&mm, "RICK", "ETH");
    // Address has enough RICK, but doesn't have ETH, so setprice call should fail because maker will not have gas to spend ERC20 taker payment.
    check_set_price_fails(&mm, "RICK", "JST");

    // Expect error as MORTY balance is 0
    check_buy_fails(&mm, "RICK", "MORTY", 0.1);
    // RICK balance is sufficient, but amount is too small, it will result to dust error from RPC
    check_buy_fails(&mm, "MORTY", "RICK", 0.000001);
    // Address has enough RICK, but doesn't have ETH, so buy call should fail because taker will not have gas to spend ETH maker payment.
    check_buy_fails(&mm, "ETH", "RICK", 0.1);
    // Address has enough RICK, but doesn't have ETH, so buy call should fail because taker will not have gas to spend ERC20 maker payment.
    check_buy_fails(&mm, "JST", "RICK", 0.1);

    // Expect error as MORTY balance is 0
    check_sell_fails(&mm, "MORTY", "RICK", 0.1);
    // RICK balance is sufficient, but amount is too small, the dex fee will result to dust error from RPC
    check_sell_fails(&mm, "RICK", "MORTY", 0.000001);
    // Address has enough RICK, but doesn't have ETH, so buy call should fail because taker will not have gas to spend ETH maker payment.
    check_sell_fails(&mm, "RICK", "ETH", 0.1);
    // Address has enough RICK, but doesn't have ETH, so buy call should fail because taker will not have gas to spend ERC20 maker payment.
    check_sell_fails(&mm, "RICK", "JST", 0.1);
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_rpc_password_from_json() {
    let coins = json!([
        {"coin":"RICK","asset":"RICK","rpcport":8923,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","rpcport":8923,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
    ]);

    // do not allow empty password
    let mut err_mm1 = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "passphrase": "bob passphrase",
            "coins": coins,
            "rpc_password": "",
            "i_am_seed": true,
            "skip_startup_checks": true,
        }),
        "password".into(),
        local_start!("bob"),
    )
    .unwrap();
    block_on(err_mm1.wait_for_log(5., |log| log.contains("rpc_password must not be empty"))).unwrap();

    // do not allow empty password
    let mut err_mm2 = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "passphrase": "bob passphrase",
            "coins": coins,
            "rpc_password": {"key":"value"},
            "i_am_seed": true,
            "skip_startup_checks": true,
        }),
        "password".into(),
        local_start!("bob"),
    )
    .unwrap();
    block_on(err_mm2.wait_for_log(5., |log| log.contains("rpc_password must be string"))).unwrap();

    let mm = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "passphrase": "bob passphrase",
            "coins": coins,
            "rpc_password": "password",
            "i_am_seed": true,
        }),
        "password".into(),
        local_start!("bob"),
    )
    .unwrap();
    let (_dump_log, _dump_dashboard) = mm.mm_dump();
    log!({"Log path: {}", mm.log_path.display()});
    let electrum_invalid = block_on(mm.rpc(json! ({
        "userpass": "password1",
        "method": "electrum",
        "coin": "RICK",
        "servers": [{"url":"electrum1.cipig.net:10017"},{"url":"electrum2.cipig.net:10017"},{"url":"electrum3.cipig.net:10017"}],
        "mm2": 1,
    }))).unwrap();

    // electrum call must fail if invalid password is provided
    assert!(
        electrum_invalid.0.is_server_error(),
        "RPC «electrum» should have failed with server error, but got «{}», response «{}»",
        electrum_invalid.0,
        electrum_invalid.1
    );

    let electrum = block_on(mm.rpc (json! ({
        "userpass": mm.userpass,
        "method": "electrum",
        "coin": "RICK",
        "servers": [{"url":"electrum1.cipig.net:10017"},{"url":"electrum2.cipig.net:10017"},{"url":"electrum3.cipig.net:10017"}],
        "mm2": 1,
    }))).unwrap();

    // electrum call must be successful with RPC password from config
    assert_eq!(
        electrum.0,
        StatusCode::OK,
        "RPC «electrum» failed with status «{}», response «{}»",
        electrum.0,
        electrum.1
    );

    let electrum = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "electrum",
        "coin": "MORTY",
        "servers": [{"url":"electrum1.cipig.net:10018"},{"url":"electrum2.cipig.net:10018"},{"url":"electrum3.cipig.net:10018"}],
        "mm2": 1,
    }))).unwrap();

    // electrum call must be successful with RPC password from config
    assert_eq!(
        electrum.0,
        StatusCode::OK,
        "RPC «electrum» failed with status «{}», response «{}»",
        electrum.0,
        electrum.1
    );

    let orderbook = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "orderbook",
        "base": "RICK",
        "rel": "MORTY",
    })))
    .unwrap();

    // orderbook call must be successful with RPC password from config
    assert_eq!(
        orderbook.0,
        StatusCode::OK,
        "RPC «orderbook» failed with status «{}», response «{}»",
        orderbook.0,
        orderbook.1
    );
}

/// Currently only `withdraw` RPC call supports V2.
#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_mmrpc_v2() {
    let coins = json!([
        {"coin":"RICK","asset":"RICK","rpcport":8923,"txversion":4,"protocol":{"type":"UTXO"}},
    ]);

    let mm = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "passphrase": "bob passphrase",
            "coins": coins,
            "rpc_password": "password",
            "i_am_seed": true,
        }),
        "password".into(),
        local_start!("bob"),
    )
    .unwrap();
    let (_dump_log, _dump_dashboard) = mm.mm_dump();
    log!({"Log path: {}", mm.log_path.display()});

    let _electrum = block_on(enable_electrum(&mm, "RICK", false, &[
        "electrum3.cipig.net:10017",
        "electrum2.cipig.net:10017",
        "electrum1.cipig.net:10017",
    ]));

    // no `userpass`
    let withdraw = block_on(mm.rpc(json! ({
        "mmrpc": "2.0",
        "method": "withdraw",
        "params": {
            "coin": "RICK",
            "to": "RJTYiYeJ8eVvJ53n2YbrVmxWNNMVZjDGLh",
            "amount": 0.001,
        },
    })))
    .unwrap();
    assert!(
        withdraw.0.is_client_error(),
        "withdraw should have failed, but got: {}",
        withdraw.1
    );
    let withdraw_error: RpcErrorResponse<()> = json::from_str(&withdraw.1).expect("Expected 'RpcErrorResponse'");
    assert_eq!(withdraw_error.error_type, "UserpassIsNotSet");
    assert!(withdraw_error.error_data.is_none());

    // invalid `userpass`
    let withdraw = block_on(mm.rpc(json! ({
        "mmrpc": "2.0",
        "userpass": "another password",
        "method": "withdraw",
        "params": {
            "coin": "RICK",
            "to": "RJTYiYeJ8eVvJ53n2YbrVmxWNNMVZjDGLh",
            "amount": 0.001,
        },
    })))
    .unwrap();
    assert!(
        withdraw.0.is_client_error(),
        "withdraw should have failed, but got: {}",
        withdraw.1
    );
    let withdraw_error: RpcErrorResponse<Json> = json::from_str(&withdraw.1).expect("Expected 'RpcErrorResponse'");
    assert_eq!(withdraw_error.error_type, "UserpassIsInvalid");
    assert!(withdraw_error.error_data.is_none());

    // invalid `mmrpc` version
    let withdraw = block_on(mm.rpc(json! ({
        "mmrpc": "1.0",
        "userpass": mm.userpass,
        "method": "withdraw",
        "params": {
            "coin": "RICK",
            "to": "RJTYiYeJ8eVvJ53n2YbrVmxWNNMVZjDGLh",
            "amount": 0.001,
        },
    })))
    .unwrap();
    assert!(
        withdraw.0.is_client_error(),
        "withdraw should have failed, but got: {}",
        withdraw.1
    );
    log!([withdraw.1]);
    let withdraw_error: RpcErrorResponse<String> = json::from_str(&withdraw.1).expect("Expected 'RpcErrorResponse'");
    assert_eq!(withdraw_error.error_type, "InvalidMmRpcVersion");

    // 'id' = 3
    let withdraw = block_on(mm.rpc(json! ({
        "mmrpc": "2.0",
        "userpass": mm.userpass,
        "method": "withdraw",
        "params": {
            "coin": "RICK",
            "to": "RJTYiYeJ8eVvJ53n2YbrVmxWNNMVZjDGLh",
            "amount": 0.001,
        },
        "id": 3,
    })))
    .unwrap();
    assert!(withdraw.0.is_success(), "!withdraw: {}", withdraw.1);
    let withdraw_ok: RpcSuccessResponse<TransactionDetails> =
        json::from_str(&withdraw.1).expect("Expected 'RpcSuccessResponse<TransactionDetails>'");
    assert_eq!(withdraw_ok.id, Some(3));
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_rpc_password_from_json_no_userpass() {
    let coins = json!([
        {"coin":"RICK","asset":"RICK","rpcport":8923,"txversion":4,"protocol":{"type":"UTXO"}},
    ]);

    let mm = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "passphrase": "bob passphrase",
            "coins": coins,
            "i_am_seed": true,
        }),
        "password".into(),
        local_start!("bob"),
    )
    .unwrap();
    let (_dump_log, _dump_dashboard) = mm.mm_dump();
    log!({"Log path: {}", mm.log_path.display()});
    let electrum = block_on(mm.rpc(json! ({
        "method": "electrum",
        "coin": "RICK",
        "urls": ["electrum2.cipig.net:10017"],
    })))
    .unwrap();

    // electrum call must return 500 status code
    assert!(
        electrum.0.is_server_error(),
        "RPC «electrum» should have failed with server error, but got «{}», response «{}»",
        electrum.0,
        electrum.1
    );
}

/// Trading test using coins with remote RPC (Electrum, ETH nodes), it needs only ENV variables to be set, coins daemons are not required.
/// Trades few pairs concurrently to speed up the process and also act like "load" test
async fn trade_base_rel_electrum(
    pairs: &[(&'static str, &'static str)],
    maker_price: i32,
    taker_price: i32,
    volume: f64,
) {
    let bob_passphrase = get_passphrase!(".env.seed", "BOB_PASSPHRASE").unwrap();
    let alice_passphrase = get_passphrase!(".env.client", "ALICE_PASSPHRASE").unwrap();

    let coins = json! ([
        {"coin":"RICK","asset":"RICK","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"}},
        {"coin":"ZOMBIE","asset":"ZOMBIE","fname":"ZOMBIE (TESTCOIN)","txversion":4,"overwintered":1,"mm2":1,"protocol":{"type":"ZHTLC"}},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}}}
    ]);

    let mut mm_bob = MarketMakerIt::start_async(
        json! ({
            "gui": "nogui",
            "netid": 8999,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": bob_passphrase,
            "coins": coins,
            "rpc_password": "password",
            "i_am_seed": true,
        }),
        "password".into(),
        local_start!("bob"),
    )
    .await
    .unwrap();

    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    #[cfg(not(target_arch = "wasm32"))]
    {
        log! ({"Bob log path: {}", mm_bob.log_path.display()})
    }

    Timer::sleep(1.).await;

    // Both Alice and Bob might try to bind on the "0.0.0.0:47773" DHT port in this test
    // (because the local "127.0.0.*:47773" addresses aren't that useful for DHT).
    // We want to give Bob a headstart in acquiring the port,
    // because Alice will then be able to directly reach it (thanks to "seednode").
    // Direct communication is not required in this test, but it's nice to have.
    // wait_log_re! (mm_bob, 9., "preferred port");

    let mut mm_alice = MarketMakerIt::start_async(
        json! ({
            "gui": "nogui",
            "netid": 8999,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("ALICE_TRADE_IP") .ok(),
            "rpcip": env::var ("ALICE_TRADE_IP") .ok(),
            "passphrase": alice_passphrase,
            "coins": coins,
            "seednodes": [mm_bob.my_seed_addr()],
            "rpc_password": "password",
            "skip_startup_checks": true,
        }),
        "password".into(),
        local_start!("alice"),
    )
    .await
    .unwrap();

    let (_alice_dump_log, _alice_dump_dashboard) = mm_alice.mm_dump();
    #[cfg(not(target_arch = "wasm32"))]
    {
        log! ({"Alice log path: {}", mm_alice.log_path.display()})
    }

    // Enable coins on Bob side. Print the replies in case we need the address.
    let rc = enable_coins_eth_electrum(&mm_bob, &["http://195.201.0.6:8565"]).await;
    log! ({"enable_coins (bob): {:?}", rc});

    // Enable coins on Alice side. Print the replies in case we need the address.
    let rc = enable_coins_eth_electrum(&mm_alice, &["http://195.201.0.6:8565"]).await;
    log! ({"enable_coins (alice): {:?}", rc});

    // unwrap! (mm_alice.wait_for_log (999., &|log| log.contains ("set pubkey for ")));

    let mut uuids = vec![];

    // issue sell request on Bob side by setting base/rel price
    for (base, rel) in pairs.iter() {
        log!("Issue bob " (base) "/" (rel) " sell request");
        let rc = mm_bob
            .rpc(json! ({
                "userpass": mm_bob.userpass,
                "method": "setprice",
                "base": base,
                "rel": rel,
                "price": maker_price,
                "volume": volume
            }))
            .await
            .unwrap();
        assert!(rc.0.is_success(), "!setprice: {}", rc.1);
    }

    for (base, rel) in pairs.iter() {
        common::log::info!(
            "Trigger alice subscription to {}/{} orderbook topic first and sleep for 1 second",
            base,
            rel
        );
        let rc = mm_alice
            .rpc(json! ({
                "userpass": mm_alice.userpass,
                "method": "orderbook",
                "base": base,
                "rel": rel,
            }))
            .await
            .unwrap();
        assert!(rc.0.is_success(), "!orderbook: {}", rc.1);
        Timer::sleep(1.).await;
        common::log::info!("Issue alice {}/{} buy request", base, rel);
        let rc = mm_alice
            .rpc(json! ({
                "userpass": mm_alice.userpass,
                "method": "buy",
                "base": base,
                "rel": rel,
                "volume": volume,
                "price": taker_price
            }))
            .await
            .unwrap();
        assert!(rc.0.is_success(), "!buy: {}", rc.1);
        let buy_json: Json = serde_json::from_str(&rc.1).unwrap();
        uuids.push(buy_json["result"]["uuid"].as_str().unwrap().to_owned());
    }

    for (base, rel) in pairs.iter() {
        // ensure the swaps are started
        let expected_log = format!("Entering the taker_swap_loop {}/{}", base, rel);
        mm_alice
            .wait_for_log(5., |log| log.contains(&expected_log))
            .await
            .unwrap();
        let expected_log = format!("Entering the maker_swap_loop {}/{}", base, rel);
        mm_bob
            .wait_for_log(5., |log| log.contains(&expected_log))
            .await
            .unwrap()
    }

    // TODO remove this when the swap db is fully functional
    #[cfg(target_arch = "wasm32")]
    if 1 == 1 {
        for uuid in uuids.iter() {
            mm_bob
                .wait_for_log(300., |log| log.contains(&format!("[swap uuid={}] Finished", uuid)))
                .await
                .unwrap();

            mm_alice
                .wait_for_log(300., |log| log.contains(&format!("[swap uuid={}] Finished", uuid)))
                .await
                .unwrap();
        }
        return;
    }

    for uuid in uuids.iter() {
        // ensure the swaps are indexed to the SQLite database
        let expected_log = format!("Inserting new swap {} to the SQLite database", uuid);
        mm_alice
            .wait_for_log(5., |log| log.contains(&expected_log))
            .await
            .unwrap();
        mm_bob
            .wait_for_log(5., |log| log.contains(&expected_log))
            .await
            .unwrap()
    }

    for uuid in uuids.iter() {
        mm_bob
            .wait_for_log(300., |log| log.contains(&format!("[swap uuid={}] Finished", uuid)))
            .await
            .unwrap();

        mm_alice
            .wait_for_log(300., |log| log.contains(&format!("[swap uuid={}] Finished", uuid)))
            .await
            .unwrap();

        log!("Waiting a few second for the fresh swap status to be saved..");
        Timer::sleep(7.77).await;

        log!("Checking alice/taker status..");
        check_my_swap_status(
            &mm_alice,
            &uuid,
            &TAKER_SUCCESS_EVENTS,
            &TAKER_ERROR_EVENTS,
            volume.into(),
            volume.into(),
        )
        .await;

        log!("Checking bob/maker status..");
        check_my_swap_status(
            &mm_bob,
            &uuid,
            &MAKER_SUCCESS_EVENTS,
            &MAKER_ERROR_EVENTS,
            volume.into(),
            volume.into(),
        )
        .await;
    }

    log!("Waiting 3 seconds for nodes to broadcast their swaps data..");
    Timer::sleep(3.).await;

    for uuid in uuids.iter() {
        log!("Checking alice status..");
        check_stats_swap_status(&mm_alice, &uuid, &MAKER_SUCCESS_EVENTS, &TAKER_SUCCESS_EVENTS).await;

        log!("Checking bob status..");
        check_stats_swap_status(&mm_bob, &uuid, &MAKER_SUCCESS_EVENTS, &TAKER_SUCCESS_EVENTS).await;
    }

    log!("Checking alice recent swaps..");
    check_recent_swaps(&mm_alice, uuids.len()).await;
    log!("Checking bob recent swaps..");
    check_recent_swaps(&mm_bob, uuids.len()).await;
    for (base, rel) in pairs.iter() {
        log!("Get " (base) "/" (rel) " orderbook");
        let rc = mm_bob
            .rpc(json! ({
                "userpass": mm_bob.userpass,
                "method": "orderbook",
                "base": base,
                "rel": rel,
            }))
            .await
            .unwrap();
        assert!(rc.0.is_success(), "!orderbook: {}", rc.1);

        let bob_orderbook: OrderbookResponse = json::from_str(&rc.1).unwrap();
        log!((base) "/" (rel) " orderbook " [bob_orderbook]);

        assert_eq!(0, bob_orderbook.bids.len(), "{} {} bids must be empty", base, rel);
        assert_eq!(0, bob_orderbook.asks.len(), "{} {} asks must be empty", base, rel);
    }
    mm_bob.stop().await.unwrap();
    mm_alice.stop().await.unwrap();
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn trade_test_electrum_and_eth_coins() {
    let pairs: &[_] = if cfg!(feature = "zhtlc") {
        &[("ETH", "JST"), ("RICK", "ZOMBIE")]
    } else {
        &[("ETH", "JST")]
    };
    block_on(trade_base_rel_electrum(pairs, 1, 2, 0.1));
}

#[cfg(target_arch = "wasm32")]
#[no_mangle]
pub extern "C" fn trade_test_electrum_and_eth_coins(cb_id: i32) {
    use std::ptr::null;

    common::executor::spawn(async move {
        let pairs = [("ETH", "JST")];
        trade_base_rel_electrum(&pairs, 1, 2, 0.1).await;
        call_back(cb_id, null(), 0)
    })
}

#[wasm_bindgen_test]
#[cfg(target_arch = "wasm32")]
async fn trade_test_rick_and_morty() {
    let pairs: &[_] = &[("RICK", "MORTY")];
    trade_base_rel_electrum(pairs, 1, 1, 0.0001).await;
}

#[cfg(not(target_arch = "wasm32"))]
fn withdraw_and_send(
    mm: &MarketMakerIt,
    coin: &str,
    to: &str,
    enable_res: &HashMap<&'static str, EnableElectrumResponse>,
    expected_bal_change: &str,
    amount: f64,
) {
    let withdraw = block_on(mm.rpc(json! ({
        "mmrpc": "2.0",
        "userpass": mm.userpass,
        "method": "withdraw",
        "params": {
            "coin": coin,
            "to": to,
            "amount": amount,
        },
        "id": 0,
    })))
    .unwrap();

    assert!(withdraw.0.is_success(), "!withdraw: {}", withdraw.1);
    let res: RpcSuccessResponse<TransactionDetails> =
        json::from_str(&withdraw.1).expect("Expected 'RpcSuccessResponse<TransactionDetails>'");
    let tx_details = res.result;

    let from = addr_from_enable(enable_res, coin).to_owned();
    let expected_bal_change = BigDecimal::from_str(expected_bal_change).expect("!BigDecimal::from_str");

    assert_eq!(tx_details.to, vec![to.to_owned()]);
    assert_eq!(tx_details.my_balance_change, expected_bal_change);
    assert_eq!(tx_details.from, vec![from]);

    let send = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "send_raw_transaction",
        "coin": coin,
        "tx_hex": tx_details.tx_hex,
    })))
    .unwrap();
    assert!(send.0.is_success(), "!{} send: {}", coin, send.1);
    let send_json: Json = json::from_str(&send.1).unwrap();
    assert_eq!(tx_details.tx_hash, send_json["tx_hash"]);
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_withdraw_and_send() {
    let (alice_file_passphrase, _alice_file_userpass) = from_env_file(slurp(&".env.client").unwrap());

    let alice_passphrase = var("ALICE_PASSPHRASE")
        .ok()
        .or(alice_file_passphrase)
        .expect("No ALICE_PASSPHRASE or .env.client/PASSPHRASE");

    let coins = json! ([
        {"coin":"RICK","asset":"RICK","rpcport":8923,"txversion":4,"overwintered":1,"txfee":1000,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","rpcport":8923,"txversion":4,"overwintered":1,"txfee":1000,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY_SEGWIT","asset":"MORTY_SEGWIT","txversion":4,"overwintered":1,"segwit":true,"txfee":1000,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"}},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}}}
    ]);

    let mm_alice = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 8100,
            "myipaddr": env::var ("ALICE_TRADE_IP") .ok(),
            "rpcip": env::var ("ALICE_TRADE_IP") .ok(),
            "passphrase": alice_passphrase,
            "coins": coins,
            "rpc_password": "password",
            "i_am_seed": true,
        }),
        "password".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "alice" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();

    let (_alice_dump_log, _alice_dump_dashboard) = mm_alice.mm_dump();
    log! ({"Alice log path: {}", mm_alice.log_path.display()});

    // wait until RPC API is active

    // Enable coins. Print the replies in case we need the address.
    let mut enable_res = block_on(enable_coins_eth_electrum(&mm_alice, &["http://195.201.0.6:8565"]));
    enable_res.insert(
        "MORTY_SEGWIT",
        block_on(enable_electrum(&mm_alice, "MORTY_SEGWIT", false, &[
            "electrum1.cipig.net:10018",
            "electrum2.cipig.net:10018",
            "electrum3.cipig.net:10018",
        ])),
    );

    log!("enable_coins (alice): "[enable_res]);
    withdraw_and_send(
        &mm_alice,
        "MORTY",
        "RJTYiYeJ8eVvJ53n2YbrVmxWNNMVZjDGLh",
        &enable_res,
        "-0.00101",
        0.001,
    );
    // dev chain gas price is 0 so ETH expected balance change doesn't include the fee
    withdraw_and_send(
        &mm_alice,
        "ETH",
        "0x657980d55733B41c0C64c06003864e1aAD917Ca7",
        &enable_res,
        "-0.001",
        0.001,
    );
    withdraw_and_send(
        &mm_alice,
        "JST",
        "0x657980d55733B41c0C64c06003864e1aAD917Ca7",
        &enable_res,
        "-0.001",
        0.001,
    );

    // must not allow to withdraw to non-P2PKH addresses
    let withdraw = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "mmrpc": "2.0",
        "method": "withdraw",
        "params": {
            "coin": "MORTY",
            "to": "bUN5nesdt1xsAjCtAaYUnNbQhGqUWwQT1Q",
            "amount": "0.001",
        },
        "id": 0,
    })))
    .unwrap();

    assert!(withdraw.0.is_client_error(), "MORTY withdraw: {}", withdraw.1);
    let res: RpcErrorResponse<String> = json::from_str(&withdraw.1).unwrap();
    assert_eq!(res.error_type, "InvalidAddress");
    assert!(res
        .error_data
        .unwrap()
        .contains("Expected a valid P2PKH or P2SH prefix for MORTY"));

    // but must allow to withdraw to P2SH addresses if Segwit flag is true
    let withdraw = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "mmrpc": "2.0",
        "method": "withdraw",
        "params": {
            "coin": "MORTY_SEGWIT",
            "to": "bUN5nesdt1xsAjCtAaYUnNbQhGqUWwQT1Q",
            "amount": "0.001",
        },
        "id": 0,
    })))
    .unwrap();

    assert!(withdraw.0.is_success(), "MORTY_SEGWIT withdraw: {}", withdraw.1);

    // must not allow to withdraw to invalid checksum address
    let withdraw = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "mmrpc": "2.0",
        "method": "withdraw",
        "params": {
            "coin": "ETH",
            "to": "0x657980d55733b41c0c64c06003864e1aad917ca7",
            "amount": "0.001",
        },
        "id": 0,
    })))
    .unwrap();

    assert!(withdraw.0.is_client_error(), "ETH withdraw: {}", withdraw.1);
    let res: RpcErrorResponse<String> = json::from_str(&withdraw.1).unwrap();
    assert_eq!(res.error_type, "InvalidAddress");
    assert!(res.error.contains("Invalid address checksum"));

    // must not allow to withdraw too small amount 0.000005 (less than 0.00001 dust)
    let small_amount = MmNumber::from("0.000005").to_decimal();
    let withdraw = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "mmrpc": "2.0",
        "method": "withdraw",
        "params": {
            "coin": "MORTY",
            "to": "RHzSYSHv3G6J8xL3MyGH3y2gU588VCTC7X",
            "amount": small_amount,
        },
        "id": 0,
    })))
    .unwrap();

    assert!(withdraw.0.is_client_error(), "MORTY withdraw: {}", withdraw.1);
    log!("error: "[withdraw.1]);
    let error: RpcErrorResponse<withdraw_error::AmountTooLow> = json::from_str(&withdraw.1).unwrap();
    let threshold = MmNumber::from("0.00001").to_decimal();
    let expected_error = withdraw_error::AmountTooLow {
        amount: small_amount,
        threshold,
    };
    assert_eq!(error.error_type, "AmountTooLow");
    assert_eq!(error.error_data, Some(expected_error));

    block_on(mm_alice.stop()).unwrap();
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_tbtc_withdraw_to_cashaddresses_should_fail() {
    let seed = "spice describe gravity federal blast come thank unfair canal monkey style afraid";

    let coins = json! ([
        {
            "coin": "tBTC",
            "name": "tbitcoin",
            "fname": "tBitcoin",
            "rpcport": 18332,
            "pubtype": 111,
            "p2shtype": 196,
            "wiftype": 239,
            "segwit": true,
            "bech32_hrp": "tb",
            "txfee": 1000,
            "mm2": 1,
            "required_confirmations": 0,
            "protocol": {
                "type": "UTXO"
            }
        }
    ]);

    let mm_alice = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 8100,
            "myipaddr": env::var ("ALICE_TRADE_IP") .ok(),
            "rpcip": env::var ("ALICE_TRADE_IP") .ok(),
            "passphrase": seed.to_string(),
            "coins": coins,
            "rpc_password": "password",
            "i_am_seed": true,
        }),
        "password".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "alice" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();

    let (_alice_dump_log, _alice_dump_dashboard) = mm_alice.mm_dump();
    log! ({"Alice log path: {}", mm_alice.log_path.display()});

    // wait until RPC API is active

    // Enable coins. Print the replies in case we need the address.
    let electrum = block_on(mm_alice.rpc(json!({
        "userpass": mm_alice.userpass,
        "method": "electrum",
        "coin": "tBTC",
        "servers": [{"url":"electrum1.cipig.net:10068"},{"url":"electrum2.cipig.net:10068"},{"url":"electrum3.cipig.net:10068"}],
        "mm2": 1,
    }))).unwrap();
    assert_eq!(
        electrum.0,
        StatusCode::OK,
        "RPC «electrum» failed with {} {}",
        electrum.0,
        electrum.1
    );
    log!("enable_coins (alice): "[electrum]);

    let electrum_response: EnableElectrumResponse =
        json::from_str(&electrum.1).expect("Expected 'EnableElectrumResponse'");
    let mut enable_res = HashMap::new();
    enable_res.insert("tBTC", electrum_response);

    // Send from BTC Legacy Address to Cashaddress should fail
    let withdraw = block_on(mm_alice.rpc(json!({
        "userpass": mm_alice.userpass,
        "method": "withdraw",
        "coin": "tBTC",
        "to": "bchtest:qqgp9xh3435xamv7ghct8emer2s2erzj8gx3gnhwkq",
        "amount": 0.00001,
    })))
    .unwrap();

    assert!(withdraw.0.is_server_error(), "tBTC withdraw: {}", withdraw.1);
    log!([withdraw.1]);

    block_on(mm_alice.stop()).unwrap();
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_withdraw_legacy() {
    let (alice_file_passphrase, _alice_file_userpass) = from_env_file(slurp(&".env.client").unwrap());

    let alice_passphrase = var("ALICE_PASSPHRASE")
        .ok()
        .or(alice_file_passphrase)
        .expect("No ALICE_PASSPHRASE or .env.client/PASSPHRASE");

    let coins = json!([
        {"coin":"RICK","asset":"RICK","rpcport":8923,"txversion":4,"overwintered":1,"txfee":1000,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","rpcport":8923,"txversion":4,"overwintered":1,"txfee":1000,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY_SEGWIT","asset":"MORTY_SEGWIT","txversion":4,"overwintered":1,"segwit":true,"txfee":1000,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"}},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}}}
    ]);

    let mm_alice = MarketMakerIt::start(
        json!({
            "gui": "nogui",
            "netid": 8100,
            "myipaddr": env::var ("ALICE_TRADE_IP") .ok(),
            "rpcip": env::var ("ALICE_TRADE_IP") .ok(),
            "passphrase": alice_passphrase,
            "coins": coins,
            "rpc_password": "password",
            "i_am_seed": true,
        }),
        "password".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "alice" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();

    let (_alice_dump_log, _alice_dump_dashboard) = mm_alice.mm_dump();
    log!({ "Alice log path: {}", mm_alice.log_path.display() });

    // wait until RPC API is active

    // Enable coins. Print the replies in case we need the address.
    let mut enable_res = block_on(enable_coins_eth_electrum(&mm_alice, &["http://195.201.0.6:8565"]));
    enable_res.insert(
        "MORTY_SEGWIT",
        block_on(enable_electrum(&mm_alice, "MORTY_SEGWIT", false, &[
            "electrum1.cipig.net:10018",
            "electrum2.cipig.net:10018",
            "electrum3.cipig.net:10018",
        ])),
    );
    log!("enable_coins (alice): "[enable_res]);

    let withdraw = block_on(mm_alice.rpc(json!({
        "userpass": mm_alice.userpass,
        "method": "withdraw",
        "coin": "MORTY",
        "to": "RJTYiYeJ8eVvJ53n2YbrVmxWNNMVZjDGLh",
        "amount": 0.001,
    })))
    .unwrap();
    assert!(withdraw.0.is_success(), "MORTY withdraw: {}", withdraw.1);
    let _: TransactionDetails = json::from_str(&withdraw.1).expect("Expected 'TransactionDetails'");

    // must not allow to withdraw to non-P2PKH addresses
    let withdraw = block_on(mm_alice.rpc(json!({
        "userpass": mm_alice.userpass,
        "method": "withdraw",
        "coin": "MORTY",
        "to": "bUN5nesdt1xsAjCtAaYUnNbQhGqUWwQT1Q",
        "amount": "0.001",
    })))
    .unwrap();

    assert!(withdraw.0.is_server_error(), "MORTY withdraw: {}", withdraw.1);
    log!([withdraw.1]);
    let withdraw_error: Json = json::from_str(&withdraw.1).unwrap();
    withdraw_error["error"]
        .as_str()
        .expect("Expected 'error' field")
        .contains("Expected either P2PKH or P2SH");
    assert!(withdraw_error.get("error_path").is_none());
    assert!(withdraw_error.get("error_trace").is_none());
    assert!(withdraw_error.get("error_type").is_none());
    assert!(withdraw_error.get("error_data").is_none());

    block_on(mm_alice.stop()).unwrap();
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_withdraw_segwit() {
    let seed = "spice describe gravity federal blast come thank unfair canal monkey style afraid";

    let coins = json!([
        {
            "coin": "tBTC",
            "name": "tbitcoin",
            "fname": "tBitcoin",
            "rpcport": 18332,
            "pubtype": 111,
            "p2shtype": 196,
            "wiftype": 239,
            "segwit": true,
            "bech32_hrp": "tb",
            "txfee": 0,
            "estimate_fee_mode": "ECONOMICAL",
            "mm2": 1,
            "required_confirmations": 0,
            "protocol": {
                "type": "UTXO"
            }
        }
    ]);

    let mm_alice = MarketMakerIt::start(
        json!({
            "gui": "nogui",
            "netid": 8100,
            "myipaddr": env::var ("ALICE_TRADE_IP") .ok(),
            "rpcip": env::var ("ALICE_TRADE_IP") .ok(),
            "passphrase": seed.to_string(),
            "coins": coins,
            "rpc_password": "password",
            "i_am_seed": true,
        }),
        "password".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "alice" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();

    let (_alice_dump_log, _alice_dump_dashboard) = mm_alice.mm_dump();
    log!({ "Alice log path: {}", mm_alice.log_path.display() });

    // wait until RPC API is active

    // Enable coins. Print the replies in case we need the address.
    let electrum = block_on(mm_alice.rpc(json!({
        "userpass": mm_alice.userpass,
        "method": "electrum",
        "coin": "tBTC",
        "servers": [{"url":"electrum1.cipig.net:10068"},{"url":"electrum2.cipig.net:10068"},{"url":"electrum3.cipig.net:10068"}],
        "mm2": 1,
        "address_format": {
            "format": "segwit",
        },
    }))).unwrap();
    assert_eq!(
        electrum.0,
        StatusCode::OK,
        "RPC «electrum» failed with {} {}",
        electrum.0,
        electrum.1
    );
    log!("enable_coins (alice): "[electrum]);

    let withdraw = block_on(mm_alice.rpc(json!({
        "userpass": mm_alice.userpass,
        "method": "withdraw",
        "coin": "tBTC",
        "to": "tb1qdkwjk42dw6pryvs9sl0ht3pn3mxghuma64jst5",
        "amount": 0.00001,
    })))
    .unwrap();
    assert!(withdraw.0.is_success(), "tBTC withdraw: {}", withdraw.1);
    let _: TransactionDetails = json::from_str(&withdraw.1).expect("Expected 'TransactionDetails'");

    // must not allow to withdraw to addresses with different hrp
    let withdraw = block_on(mm_alice.rpc(json!({
        "userpass": mm_alice.userpass,
        "method": "withdraw",
        "coin": "tBTC",
        "to": "ltc1qdkwjk42dw6pryvs9sl0ht3pn3mxghuma64jst5",
        "amount": 0.00001,
    })))
    .unwrap();

    assert!(withdraw.0.is_server_error(), "tBTC withdraw: {}", withdraw.1);
    log!([withdraw.1]);
    let withdraw_error: Json = json::from_str(&withdraw.1).unwrap();
    withdraw_error["error"]
        .as_str()
        .expect("Expected 'error' field")
        .contains("Address hrp ltc is not a valid hrp for tBTC");
    assert!(withdraw_error.get("error_path").is_none());
    assert!(withdraw_error.get("error_trace").is_none());
    assert!(withdraw_error.get("error_type").is_none());
    assert!(withdraw_error.get("error_data").is_none());

    block_on(mm_alice.stop()).unwrap();
}

/// Ensure that swap status return the 404 status code if swap is not found
#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_swap_status() {
    let coins = json! ([{"coin":"RICK","asset":"RICK"},]);

    let mm = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 8100,
            "myipaddr": env::var ("ALICE_TRADE_IP") .ok(),
            "rpcip": env::var ("ALICE_TRADE_IP") .ok(),
            "passphrase": "some passphrase",
            "coins": coins,
            "rpc_password": "password",
            "i_am_seed": true,
        }),
        "password".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "alice" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();

    let my_swap = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "my_swap_status",
        "params": {
            "uuid":Uuid::new_v4(),
        }
    })))
    .unwrap();

    assert_eq!(
        my_swap.0,
        StatusCode::NOT_FOUND,
        "!not found status code: {}",
        my_swap.1
    );

    let stats_swap = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "stats_swap_status",
        "params": {
            "uuid":Uuid::new_v4(),
        }
    })))
    .unwrap();

    assert_eq!(
        stats_swap.0,
        StatusCode::NOT_FOUND,
        "!not found status code: {}",
        stats_swap.1
    );
}

/// Ensure that setprice/buy/sell calls deny base == rel
/// https://github.com/artemii235/SuperNET/issues/363
#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_order_errors_when_base_equal_rel() {
    let coins = json!([
        {"coin":"RICK","asset":"RICK","rpcport":8923,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
    ]);

    let mm = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": "bob passphrase",
            "coins": coins,
            "rpc_password": "pass",
            "i_am_seed": true,
        }),
        "pass".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "bob" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();
    let (_dump_log, _dump_dashboard) = mm.mm_dump();
    log!({"Log path: {}", mm.log_path.display()});
    block_on(enable_electrum(&mm, "RICK", false, &[
        "electrum3.cipig.net:10017",
        "electrum2.cipig.net:10017",
        "electrum1.cipig.net:10017",
    ]));

    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "setprice",
        "base": "RICK",
        "rel": "RICK",
        "price": 0.9
    })))
    .unwrap();
    assert!(rc.0.is_server_error(), "setprice should have failed, but got {:?}", rc);

    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "buy",
        "base": "RICK",
        "rel": "RICK",
        "price": 0.9,
        "relvolume": 0.1,
    })))
    .unwrap();
    assert!(rc.0.is_server_error(), "buy should have failed, but got {:?}", rc);

    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "sell",
        "base": "RICK",
        "rel": "RICK",
        "price": 0.9,
        "basevolume": 0.1,
    })))
    .unwrap();
    assert!(rc.0.is_server_error(), "sell should have failed, but got {:?}", rc);
}

#[cfg(not(target_arch = "wasm32"))]
fn startup_passphrase(passphrase: &str, expected_address: &str) {
    let coins = json!([
        {"coin":"KMD","rpcport":8923,"txversion":4,"protocol":{"type":"UTXO"}},
    ]);

    let mm = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": passphrase,
            "coins": coins,
            "rpc_password": "pass",
            "i_am_seed": true,
        }),
        "pass".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "bob" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();
    let (_dump_log, _dump_dashboard) = mm.mm_dump();
    #[cfg(not(target_arch = "wasm32"))]
    {
        log!({"Log path: {}", mm.log_path.display()})
    }
    let enable = block_on(enable_electrum(&mm, "KMD", false, &["electrum1.cipig.net:10001"]));
    assert_eq!(expected_address, enable.address);
    block_on(mm.stop()).unwrap();
}

/// MM2 should detect if passphrase is WIF or 0x-prefixed hex encoded privkey and parse it properly.
/// https://github.com/artemii235/SuperNET/issues/396
#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_startup_passphrase() {
    // seed phrase
    startup_passphrase("bob passphrase", "RRnMcSeKiLrNdbp91qNVQwwXx5azD4S4CD");

    // WIF
    assert!(key_pair_from_seed("UvCjJf4dKSs2vFGVtCnUTAhR5FTZGdg43DDRa9s7s5DV1sSDX14g").is_ok());
    startup_passphrase(
        "UvCjJf4dKSs2vFGVtCnUTAhR5FTZGdg43DDRa9s7s5DV1sSDX14g",
        "RRnMcSeKiLrNdbp91qNVQwwXx5azD4S4CD",
    );
    // WIF, Invalid network version
    assert!(key_pair_from_seed("92Qba5hnyWSn5Ffcka56yMQauaWY6ZLd91Vzxbi4a9CCetaHtYj").is_err());
    // WIF, not compressed
    assert!(key_pair_from_seed("5HpHagT65TZzG1PH3CSu63k8DbpvD8s5ip4nEB3kEsreAnchuDf").is_err());

    // 0x prefixed hex
    assert!(key_pair_from_seed("0xb8c774f071de08c7fd8f62b97f1a5726f6ce9f1bcf141b70b86689254ed6714e").is_ok());
    startup_passphrase(
        "0xb8c774f071de08c7fd8f62b97f1a5726f6ce9f1bcf141b70b86689254ed6714e",
        "RRnMcSeKiLrNdbp91qNVQwwXx5azD4S4CD",
    );
    // Out of range, https://en.bitcoin.it/wiki/Private_key#Range_of_valid_ECDSA_private_keys
    assert!(key_pair_from_seed("0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141").is_err());
}

/// https://github.com/artemii235/SuperNET/issues/398
#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_cancel_order() {
    let coins = json!([
        {"coin":"RICK","asset":"RICK","rpcport":8923,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","rpcport":11608,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","rpcport":80,"protocol":{"type":"ETH"}},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0xc0eb7AeD740E1796992A08962c15661bDEB58003"}}}
    ]);
    let bob_passphrase = "bob passphrase";

    // start bob and immediately place the order
    let mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": bob_passphrase,
            "coins": coins,
            "i_am_seed": true,
            "rpc_password": "pass",
        }),
        "pass".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "bob" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();
    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log!({"Bob log path: {}", mm_bob.log_path.display()});
    // Enable coins on Bob side. Print the replies in case we need the "address".
    log! ({"enable_coins (bob): {:?}", block_on (enable_coins_eth_electrum (&mm_bob, &["https://ropsten.infura.io/v3/c01c1b4cf66642528547624e1d6d9d6b"]))});

    log!("Issue sell request on Bob side by setting base/rel price…");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "setprice",
        "base": "RICK",
        "rel": "MORTY",
        "price": 0.9,
        "volume": "0.9",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!setprice: {}", rc.1);
    let setprice_json: Json = json::from_str(&rc.1).unwrap();
    log!([setprice_json]);

    let mm_alice = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("ALICE_TRADE_IP") .ok(),
            "rpcip": env::var ("ALICE_TRADE_IP") .ok(),
            "passphrase": "alice passphrase",
            "coins": coins,
            "seednodes": [fomat!((mm_bob.ip))],
            "rpc_password": "pass",
        }),
        "pass".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "alice" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();

    let (_alice_dump_log, _alice_dump_dashboard) = mm_alice.mm_dump();
    log!({"Alice log path: {}", mm_alice.log_path.display()});

    // Enable coins on Alice side. Print the replies in case we need the "address".
    log! ({"enable_coins (alice): {:?}", block_on (enable_coins_eth_electrum (&mm_alice, &["https://ropsten.infura.io/v3/c01c1b4cf66642528547624e1d6d9d6b"]))});

    log!("Get RICK/MORTY orderbook on Alice side");
    let rc = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "method": "orderbook",
        "base": "RICK",
        "rel": "MORTY",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!orderbook: {}", rc.1);

    let alice_orderbook: OrderbookResponse = json::from_str(&rc.1).unwrap();
    log!("Alice orderbook "[alice_orderbook]);
    assert_eq!(
        alice_orderbook.asks.len(),
        1,
        "Alice RICK/MORTY orderbook must have exactly 1 ask"
    );

    let cancel_rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "cancel_order",
        "uuid": setprice_json["result"]["uuid"],
    })))
    .unwrap();
    assert!(cancel_rc.0.is_success(), "!cancel_order: {}", rc.1);
    let uuid: Uuid = json::from_value(setprice_json["result"]["uuid"].clone()).unwrap();
    let order_path = mm_bob.folder.join(format!(
        "DB/{}/ORDERS/MY/MAKER/{}.json",
        hex::encode(rmd160_from_passphrase(bob_passphrase)),
        uuid
    ));
    assert!(!order_path.exists());

    let pause = 3;
    log!("Waiting (" (pause) " seconds) for Bob to cancel the order…");
    thread::sleep(Duration::from_secs(pause));

    // Bob orderbook must show no orders
    log!("Get RICK/MORTY orderbook on Bob side");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "orderbook",
        "base": "RICK",
        "rel": "MORTY",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!orderbook: {}", rc.1);

    let bob_orderbook: OrderbookResponse = json::from_str(&rc.1).unwrap();
    log!("Bob orderbook "[bob_orderbook]);
    assert_eq!(bob_orderbook.asks.len(), 0, "Bob RICK/MORTY asks are not empty");

    // Alice orderbook must show no orders
    log!("Get RICK/MORTY orderbook on Alice side");
    let rc = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "method": "orderbook",
        "base": "RICK",
        "rel": "MORTY",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!orderbook: {}", rc.1);

    let alice_orderbook: OrderbookResponse = json::from_str(&rc.1).unwrap();
    log!("Alice orderbook "[alice_orderbook]);
    assert_eq!(alice_orderbook.asks.len(), 0, "Alice RICK/MORTY asks are not empty");
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_cancel_all_orders() {
    let coins = json!([
        {"coin":"RICK","asset":"RICK","rpcport":8923,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","rpcport":11608,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","rpcport":80,"protocol":{"type":"ETH"}},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0xc0eb7AeD740E1796992A08962c15661bDEB58003"}}}
    ]);

    let bob_passphrase = "bob passphrase";
    // start bob and immediately place the order
    let mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": bob_passphrase,
            "coins": coins,
            "i_am_seed": true,
            "rpc_password": "pass",
        }),
        "pass".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "bob" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();
    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log!({"Bob log path: {}", mm_bob.log_path.display()});
    // Enable coins on Bob side. Print the replies in case we need the "address".
    log! ({"enable_coins (bob): {:?}", block_on (enable_coins_eth_electrum (&mm_bob, &["https://ropsten.infura.io/v3/c01c1b4cf66642528547624e1d6d9d6b"]))});

    log!("Issue sell request on Bob side by setting base/rel price…");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "setprice",
        "base": "RICK",
        "rel": "MORTY",
        "price": 0.9,
        "volume": "0.9",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!setprice: {}", rc.1);
    let setprice_json: Json = json::from_str(&rc.1).unwrap();
    log!([setprice_json]);

    let mm_alice = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("ALICE_TRADE_IP") .ok(),
            "rpcip": env::var ("ALICE_TRADE_IP") .ok(),
            "passphrase": "alice passphrase",
            "coins": coins,
            "seednodes": [fomat!((mm_bob.ip))],
            "rpc_password": "pass",
        }),
        "pass".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "alice" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();

    let (_alice_dump_log, _alice_dump_dashboard) = mm_alice.mm_dump();
    log!({"Alice log path: {}", mm_alice.log_path.display()});

    // Enable coins on Alice side. Print the replies in case we need the "address".
    log! ({"enable_coins (alice): {:?}", block_on (enable_coins_eth_electrum (&mm_alice, &["https://ropsten.infura.io/v3/c01c1b4cf66642528547624e1d6d9d6b"]))});

    log!("Give Alice 3 seconds to import the order…");
    thread::sleep(Duration::from_secs(3));

    log!("Get RICK/MORTY orderbook on Alice side");
    let rc = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "method": "orderbook",
        "base": "RICK",
        "rel": "MORTY",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!orderbook: {}", rc.1);

    let alice_orderbook: Json = json::from_str(&rc.1).unwrap();
    log!("Alice orderbook "[alice_orderbook]);
    let asks = alice_orderbook["asks"].as_array().unwrap();
    assert_eq!(asks.len(), 1, "Alice RICK/MORTY orderbook must have exactly 1 ask");

    let cancel_rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "cancel_all_orders",
        "cancel_by": {
            "type": "All",
        }
    })))
    .unwrap();
    assert!(cancel_rc.0.is_success(), "!cancel_all_orders: {}", rc.1);
    let uuid: Uuid = json::from_value(setprice_json["result"]["uuid"].clone()).unwrap();
    let order_path = mm_bob.folder.join(format!(
        "DB/{}/ORDERS/MY/MAKER/{}.json",
        hex::encode(rmd160_from_passphrase(bob_passphrase)),
        uuid
    ));
    assert!(!order_path.exists());

    let pause = 3;
    log!("Waiting (" (pause) " seconds) for Bob to cancel the order…");
    thread::sleep(Duration::from_secs(pause));

    // Bob orderbook must show no orders
    log!("Get RICK/MORTY orderbook on Bob side");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "orderbook",
        "base": "RICK",
        "rel": "MORTY",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!orderbook: {}", rc.1);

    let bob_orderbook: Json = json::from_str(&rc.1).unwrap();
    log!("Bob orderbook "[bob_orderbook]);
    let asks = bob_orderbook["asks"].as_array().unwrap();
    assert_eq!(asks.len(), 0, "Bob RICK/MORTY asks are not empty");

    // Alice orderbook must show no orders
    log!("Get RICK/MORTY orderbook on Alice side");
    let rc = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "method": "orderbook",
        "base": "RICK",
        "rel": "MORTY",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!orderbook: {}", rc.1);

    let alice_orderbook: Json = json::from_str(&rc.1).unwrap();
    log!("Alice orderbook "[alice_orderbook]);
    let asks = alice_orderbook["asks"].as_array().unwrap();
    assert_eq!(asks.len(), 0, "Alice RICK/MORTY asks are not empty");
}

/// https://github.com/artemii235/SuperNET/issues/367
/// Electrum requests should success if at least 1 server successfully connected,
/// all others might end up with DNS resolution errors, TCP connection errors, etc.
#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_electrum_enable_conn_errors() {
    let coins = json!([
        {"coin":"RICK","asset":"RICK","protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","protocol":{"type":"UTXO"}},
    ]);

    let mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": "bob passphrase",
            "coins": coins,
            "i_am_seed": true,
            "rpc_password": "pass",
        }),
        "pass".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "bob" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();
    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log!({"Bob log path: {}", mm_bob.log_path.display()});
    // Using working servers and few else with random ports to trigger "connection refused"
    block_on(enable_electrum(&mm_bob, "RICK", false, &[
        "electrum3.cipig.net:10017",
        "electrum2.cipig.net:10017",
        "electrum1.cipig.net:10017",
        "electrum1.cipig.net:60017",
        "electrum1.cipig.net:60018",
    ]));
    // use random domain name to trigger name is not resolved
    block_on(enable_electrum(&mm_bob, "MORTY", false, &[
        "electrum3.cipig.net:10018",
        "electrum2.cipig.net:10018",
        "electrum1.cipig.net:10018",
        "random-electrum-domain-name1.net:60017",
        "random-electrum-domain-name2.net:60017",
    ]));
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_order_should_not_be_displayed_when_node_is_down() {
    let coins = json!([
        {"coin":"RICK","asset":"RICK","protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","protocol":{"type":"UTXO"}},
    ]);

    // start bob and immediately place the order
    let mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": "bob passphrase",
            "coins": coins,
            "i_am_seed": true,
            "rpc_password": "pass",
        }),
        "pass".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "bob" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();
    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log!({"Bob log path: {}", mm_bob.log_path.display()});

    log!(
        "Bob enable RICK "[block_on(enable_electrum(&mm_bob, "RICK", false, &[
            "electrum3.cipig.net:10017",
            "electrum2.cipig.net:10017",
            "electrum1.cipig.net:10017",
        ]))]
    );

    log!(
        "Bob enable MORTY "[block_on(enable_electrum(&mm_bob, "MORTY", false, &[
            "electrum3.cipig.net:10018",
            "electrum2.cipig.net:10018",
            "electrum1.cipig.net:10018",
        ]))]
    );

    let mm_alice = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("ALICE_TRADE_IP") .ok(),
            "rpcip": env::var ("ALICE_TRADE_IP") .ok(),
            "passphrase": "alice passphrase",
            "coins": coins,
            "seednodes": [fomat!((mm_bob.ip))],
            "rpc_password": "pass",
            "maker_order_timeout": 5,
        }),
        "pass".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "alice" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();

    let (_alice_dump_log, _alice_dump_dashboard) = mm_alice.mm_dump();
    log!({"Alice log path: {}", mm_alice.log_path.display()});

    log!(
        "Alice enable RICK "[block_on(enable_electrum(&mm_alice, "RICK", false, &[
            "electrum3.cipig.net:10017",
            "electrum2.cipig.net:10017",
            "electrum1.cipig.net:10017",
        ]))]
    );

    log!(
        "Alice enable MORTY "[block_on(enable_electrum(&mm_alice, "MORTY", false, &[
            "electrum3.cipig.net:10018",
            "electrum2.cipig.net:10018",
            "electrum1.cipig.net:10018",
        ]))]
    );

    // issue sell request on Bob side by setting base/rel price
    log!("Issue bob sell request");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "setprice",
        "base": "RICK",
        "rel": "MORTY",
        "price": 0.9,
        "volume": "0.9",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!setprice: {}", rc.1);

    thread::sleep(Duration::from_secs(2));

    log!("Get RICK/MORTY orderbook on Alice side");
    let rc = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "method": "orderbook",
        "base": "RICK",
        "rel": "MORTY",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!orderbook: {}", rc.1);

    let alice_orderbook: Json = json::from_str(&rc.1).unwrap();
    log!("Alice orderbook "[alice_orderbook]);
    let asks = alice_orderbook["asks"].as_array().unwrap();
    assert_eq!(asks.len(), 1, "Alice RICK/MORTY orderbook must have exactly 1 ask");

    block_on(mm_bob.stop()).unwrap();
    thread::sleep(Duration::from_secs(6));

    let rc = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "method": "orderbook",
        "base": "RICK",
        "rel": "MORTY",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!orderbook: {}", rc.1);

    let alice_orderbook: Json = json::from_str(&rc.1).unwrap();
    log!("Alice orderbook "[alice_orderbook]);
    let asks = alice_orderbook["asks"].as_array().unwrap();
    assert_eq!(asks.len(), 0, "Alice RICK/MORTY orderbook must have zero asks");

    block_on(mm_alice.stop()).unwrap();
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_own_orders_should_not_be_removed_from_orderbook() {
    let coins = json!([
        {"coin":"RICK","asset":"RICK","protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","protocol":{"type":"UTXO"}},
    ]);

    // start bob and immediately place the order
    let mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": "bob passphrase",
            "coins": coins,
            "i_am_seed": true,
            "rpc_password": "pass",
            "maker_order_timeout": 5,
        }),
        "pass".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "bob" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();
    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log!({"Bob log path: {}", mm_bob.log_path.display()});

    log!(
        "Bob enable RICK "[block_on(enable_electrum(&mm_bob, "RICK", false, &[
            "electrum3.cipig.net:10017",
            "electrum2.cipig.net:10017",
            "electrum1.cipig.net:10017",
        ]))]
    );

    log!(
        "Bob enable MORTY "[block_on(enable_electrum(&mm_bob, "MORTY", false, &[
            "electrum3.cipig.net:10018",
            "electrum2.cipig.net:10018",
            "electrum1.cipig.net:10018",
        ]))]
    );

    // issue sell request on Bob side by setting base/rel price
    log!("Issue bob sell request");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "setprice",
        "base": "RICK",
        "rel": "MORTY",
        "price": 0.9,
        "volume": "0.9",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!setprice: {}", rc.1);

    thread::sleep(Duration::from_secs(6));

    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "orderbook",
        "base": "RICK",
        "rel": "MORTY",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!orderbook: {}", rc.1);

    let bob_orderbook: Json = json::from_str(&rc.1).unwrap();
    log!("Bob orderbook "[bob_orderbook]);
    let asks = bob_orderbook["asks"].as_array().unwrap();
    assert_eq!(asks.len(), 1, "Bob RICK/MORTY orderbook must have exactly 1 ask");

    block_on(mm_bob.stop()).unwrap();
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
// https://github.com/KomodoPlatform/atomicDEX-API/issues/511
fn test_all_orders_per_pair_per_node_must_be_displayed_in_orderbook() {
    let coins = json!([
        {"coin":"RICK","asset":"RICK","protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","protocol":{"type":"UTXO"}},
    ]);

    let mm = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": "bob passphrase",
            "coins": coins,
            "rpc_password": "pass",
            "i_am_seed": true,
        }),
        "pass".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "bob" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();
    let (_dump_log, _dump_dashboard) = mm.mm_dump();
    log!({"Log path: {}", mm.log_path.display()});
    block_on(enable_electrum(&mm, "RICK", false, &[
        "electrum3.cipig.net:10017",
        "electrum2.cipig.net:10017",
        "electrum1.cipig.net:10017",
    ]));
    block_on(enable_electrum(&mm, "MORTY", false, &[
        "electrum3.cipig.net:10018",
        "electrum2.cipig.net:10018",
        "electrum1.cipig.net:10018",
    ]));

    // set 2 orders with different prices
    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "setprice",
        "base": "RICK",
        "rel": "MORTY",
        "price": 0.9,
        "volume": "0.9",
        "cancel_previous": false,
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!setprice: {}", rc.1);

    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "setprice",
        "base": "RICK",
        "rel": "MORTY",
        "price": 1,
        "volume": "0.9",
        "cancel_previous": false,
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!setprice: {}", rc.1);

    thread::sleep(Duration::from_secs(2));

    log!("Get RICK/MORTY orderbook");
    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "orderbook",
        "base": "RICK",
        "rel": "MORTY",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!orderbook: {}", rc.1);

    let orderbook: Json = json::from_str(&rc.1).unwrap();
    log!("orderbook "[orderbook]);
    let asks = orderbook["asks"].as_array().unwrap();
    assert_eq!(asks.len(), 2, "RICK/MORTY orderbook must have exactly 2 asks");
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
// https://github.com/KomodoPlatform/atomicDEX-API/issues/859
fn orderbook_extended_data() {
    let coins = json!([
        {"coin":"RICK","asset":"RICK","protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","protocol":{"type":"UTXO"}},
    ]);

    let mm = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": "bob passphrase",
            "coins": coins,
            "rpc_password": "pass",
            "i_am_seed": true,
        }),
        "pass".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "bob" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();
    let (_dump_log, _dump_dashboard) = &mm.mm_dump();
    log!({"Log path: {}", mm.log_path.display()});
    block_on(enable_electrum(&mm, "RICK", false, &[
        "electrum3.cipig.net:10017",
        "electrum2.cipig.net:10017",
        "electrum1.cipig.net:10017",
    ]));
    block_on(enable_electrum(&mm, "MORTY", false, &[
        "electrum3.cipig.net:10018",
        "electrum2.cipig.net:10018",
        "electrum1.cipig.net:10018",
    ]));

    let bob_orders = &[
        // (base, rel, price, volume)
        ("RICK", "MORTY", "0.9", "0.9"),
        ("RICK", "MORTY", "0.8", "0.9"),
        ("RICK", "MORTY", "0.7", "0.9"),
        ("MORTY", "RICK", "0.8", "0.9"),
        ("MORTY", "RICK", "1", "0.9"),
    ];

    for (base, rel, price, volume) in bob_orders {
        let rc = block_on(mm.rpc(json!({
            "userpass": mm.userpass,
            "method": "setprice",
            "base": base,
            "rel": rel,
            "price": price,
            "volume": volume,
            "cancel_previous": false,
        })))
        .unwrap();
        assert!(rc.0.is_success(), "!setprice: {}", rc.1);
    }

    thread::sleep(Duration::from_secs(1));
    log!("Get RICK/MORTY orderbook");
    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "orderbook",
        "base": "RICK",
        "rel": "MORTY",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!orderbook: {}", rc.1);

    let orderbook: OrderbookResponse = json::from_str(&rc.1).unwrap();
    log!("orderbook "[rc.1]);
    let expected_total_asks_base_vol = MmNumber::from("2.7");
    assert_eq!(expected_total_asks_base_vol.to_decimal(), orderbook.total_asks_base_vol);

    let expected_total_bids_base_vol = MmNumber::from("1.62");
    assert_eq!(expected_total_bids_base_vol.to_decimal(), orderbook.total_bids_base_vol);

    let expected_total_asks_rel_vol = MmNumber::from("2.16");
    assert_eq!(expected_total_asks_rel_vol.to_decimal(), orderbook.total_asks_rel_vol);

    let expected_total_bids_rel_vol = MmNumber::from("1.8");
    assert_eq!(expected_total_bids_rel_vol.to_decimal(), orderbook.total_bids_rel_vol);

    fn check_price_and_vol_aggr(
        order: &OrderbookEntryAggregate,
        price: &'static str,
        base_aggr: &'static str,
        rel_aggr: &'static str,
    ) {
        let price = MmNumber::from(price);
        assert_eq!(price.to_decimal(), order.price);

        let base_aggr = MmNumber::from(base_aggr);
        assert_eq!(base_aggr.to_decimal(), order.base_max_volume_aggr);

        let rel_aggr = MmNumber::from(rel_aggr);
        assert_eq!(rel_aggr.to_decimal(), order.rel_max_volume_aggr);
    }

    check_price_and_vol_aggr(&orderbook.asks[0], "0.9", "2.7", "2.16");
    check_price_and_vol_aggr(&orderbook.asks[1], "0.8", "1.8", "1.35");
    check_price_and_vol_aggr(&orderbook.asks[2], "0.7", "0.9", "0.63");

    check_price_and_vol_aggr(&orderbook.bids[0], "1.25", "0.72", "0.9");
    check_price_and_vol_aggr(&orderbook.bids[1], "1", "1.62", "1.8");
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn orderbook_should_display_base_rel_volumes() {
    let coins = json!([
        {"coin":"RICK","asset":"RICK","protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","protocol":{"type":"UTXO"}},
    ]);

    let mm = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": "bob passphrase",
            "coins": coins,
            "rpc_password": "pass",
            "i_am_seed": true,
        }),
        "pass".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "bob" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();
    let (_dump_log, _dump_dashboard) = &mm.mm_dump();
    log!({"Log path: {}", mm.log_path.display()});
    block_on(enable_electrum(&mm, "RICK", false, &[
        "electrum3.cipig.net:10017",
        "electrum2.cipig.net:10017",
        "electrum1.cipig.net:10017",
    ]));
    block_on(enable_electrum(&mm, "MORTY", false, &[
        "electrum3.cipig.net:10018",
        "electrum2.cipig.net:10018",
        "electrum1.cipig.net:10018",
    ]));

    let price = BigRational::new(2.into(), 1.into());
    let volume = BigRational::new(1.into(), 1.into());

    // create order with rational amount and price
    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "setprice",
        "base": "RICK",
        "rel": "MORTY",
        "price": price,
        "volume": volume,
        "cancel_previous": false,
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!setprice: {}", rc.1);

    thread::sleep(Duration::from_secs(1));
    log!("Get RICK/MORTY orderbook");
    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "orderbook",
        "base": "RICK",
        "rel": "MORTY",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!orderbook: {}", rc.1);

    let orderbook: OrderbookResponse = json::from_str(&rc.1).unwrap();
    log!("orderbook "[orderbook]);
    assert_eq!(orderbook.asks.len(), 1, "RICK/MORTY orderbook must have exactly 1 ask");
    let min_volume = BigRational::new(1.into(), 10000.into());
    assert_eq!(volume, orderbook.asks[0].base_max_volume_rat);
    assert_eq!(min_volume, orderbook.asks[0].base_min_volume_rat);

    assert_eq!(&volume * &price, orderbook.asks[0].rel_max_volume_rat);
    assert_eq!(&min_volume * &price, orderbook.asks[0].rel_min_volume_rat);

    log!("Get MORTY/RICK orderbook");
    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "orderbook",
        "base": "MORTY",
        "rel": "RICK",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!orderbook: {}", rc.1);

    let orderbook: OrderbookResponse = json::from_str(&rc.1).unwrap();
    log!("orderbook "[orderbook]);
    assert_eq!(orderbook.bids.len(), 1, "MORTY/RICK orderbook must have exactly 1 bid");
    let min_volume = BigRational::new(1.into(), 10000.into());
    assert_eq!(volume, orderbook.bids[0].rel_max_volume_rat);
    assert_eq!(min_volume, orderbook.bids[0].rel_min_volume_rat);

    assert_eq!(&volume * &price, orderbook.bids[0].base_max_volume_rat);
    assert_eq!(&min_volume * &price, orderbook.bids[0].base_min_volume_rat);
}

fn check_priv_key(mm: &MarketMakerIt, coin: &str, expected_priv_key: &str) {
    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "show_priv_key",
        "coin": coin
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!show_priv_key: {}", rc.1);
    let privkey: Json = json::from_str(&rc.1).unwrap();
    assert_eq!(privkey["result"]["priv_key"], Json::from(expected_priv_key))
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
// https://github.com/KomodoPlatform/atomicDEX-API/issues/519#issuecomment-589149811
fn test_show_priv_key() {
    let coins = json! ([
        {"coin":"RICK","asset":"RICK","rpcport":8923,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","rpcport":11608,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"}},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}}}
    ]);

    let mm = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": "bob passphrase",
            "coins": coins,
            "rpc_password": "pass",
            "i_am_seed": true,
        }),
        "pass".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "bob" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();

    let (_dump_log, _dump_dashboard) = mm.mm_dump();
    log!({"Log path: {}", mm.log_path.display()});
    log! ({"enable_coins: {:?}", block_on (enable_coins_eth_electrum (&mm, &["http://195.201.0.6:8565"]))});

    check_priv_key(&mm, "RICK", "UvCjJf4dKSs2vFGVtCnUTAhR5FTZGdg43DDRa9s7s5DV1sSDX14g");
    check_priv_key(
        &mm,
        "ETH",
        "0xb8c774f071de08c7fd8f62b97f1a5726f6ce9f1bcf141b70b86689254ed6714e",
    );
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
// https://github.com/KomodoPlatform/atomicDEX-API/issues/586
fn test_electrum_and_enable_response() {
    let coins = json! ([
        {"coin":"RICK","asset":"RICK","rpcport":8923,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"},"mature_confirmations":101},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"}},
    ]);

    let mm = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": "bob passphrase",
            "coins": coins,
            "rpc_password": "pass",
            "i_am_seed": true,
        }),
        "pass".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "bob" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();

    let (_dump_log, _dump_dashboard) = mm.mm_dump();
    log!({"Log path: {}", mm.log_path.display()});

    let electrum_rick = block_on(mm.rpc(json!({
        "userpass": mm.userpass,
        "method": "electrum",
        "coin": "RICK",
        "servers": [{"url":"electrum1.cipig.net:10017"},{"url":"electrum2.cipig.net:10017"},{"url":"electrum3.cipig.net:10017"}],
        "mm2": 1,
        "required_confirmations": 10,
        "requires_notarization": true
    }))).unwrap();
    assert_eq!(
        electrum_rick.0,
        StatusCode::OK,
        "RPC «electrum» failed with {} {}",
        electrum_rick.0,
        electrum_rick.1
    );
    let rick_response: Json = json::from_str(&electrum_rick.1).unwrap();
    assert_eq!(rick_response["unspendable_balance"], Json::from("0"));
    assert_eq!(rick_response["required_confirmations"], Json::from(10));
    assert_eq!(rick_response["requires_notarization"], Json::from(true));
    assert_eq!(rick_response["mature_confirmations"], Json::from(101));

    // should change requires notarization at runtime
    let requires_nota_rick = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "set_requires_notarization",
        "coin": "RICK",
        "requires_notarization": false
    })))
    .unwrap();

    assert_eq!(
        requires_nota_rick.0,
        StatusCode::OK,
        "RPC «set_requires_notarization» failed with {} {}",
        requires_nota_rick.0,
        requires_nota_rick.1
    );
    let requires_nota_rick_response: Json = json::from_str(&requires_nota_rick.1).unwrap();
    assert_eq!(
        requires_nota_rick_response["result"]["requires_notarization"],
        Json::from(false)
    );

    let enable_eth = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "enable",
        "coin": "ETH",
        "urls": ["http://195.201.0.6:8565"],
        "mm2": 1,
        "swap_contract_address": "0xa09ad3cd7e96586ebd05a2607ee56b56fb2db8fd",
        "required_confirmations": 10,
        "requires_notarization": true
    })))
    .unwrap();
    assert_eq!(
        enable_eth.0,
        StatusCode::OK,
        "RPC «enable» failed with {} {}",
        enable_eth.0,
        enable_eth.1
    );
    let eth_response: Json = json::from_str(&enable_eth.1).unwrap();
    assert_eq!(rick_response["unspendable_balance"], Json::from("0"));
    assert_eq!(eth_response["required_confirmations"], Json::from(10));
    // requires_notarization doesn't take any effect on ETH/ERC20 coins
    assert_eq!(eth_response["requires_notarization"], Json::from(false));
    // check if there is no `mature_confirmations` field
    assert_eq!(eth_response.get("mature_confirmations"), None);
}

fn check_too_low_volume_order_creation_fails(mm: &MarketMakerIt, base: &str, rel: &str) {
    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "setprice",
        "base": base,
        "rel": rel,
        "price": "1",
        "volume": "0.00000099",
        "cancel_previous": false,
    })))
    .unwrap();
    assert!(!rc.0.is_success(), "setprice success, but should be error {}", rc.1);

    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "setprice",
        "base": base,
        "rel": rel,
        "price": "0.00000099",
        "volume": "1",
        "cancel_previous": false,
    })))
    .unwrap();
    assert!(!rc.0.is_success(), "setprice success, but should be error {}", rc.1);

    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "sell",
        "base": base,
        "rel": rel,
        "price": "1",
        "volume": "0.00000099",
    })))
    .unwrap();
    assert!(!rc.0.is_success(), "sell success, but should be error {}", rc.1);

    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "buy",
        "base": base,
        "rel": rel,
        "price": "1",
        "volume": "0.00000099",
    })))
    .unwrap();
    assert!(!rc.0.is_success(), "buy success, but should be error {}", rc.1);
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
// https://github.com/KomodoPlatform/atomicDEX-API/issues/481
fn setprice_buy_sell_too_low_volume() {
    let bob_passphrase = get_passphrase(&".env.seed", "BOB_PASSPHRASE").unwrap();

    let coins = json! ([
        {"coin":"RICK","asset":"RICK","rpcport":8923,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","rpcport":11608,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"}},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}}}
    ]);

    let mm = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": bob_passphrase,
            "coins": coins,
            "rpc_password": "pass",
            "i_am_seed": true,
        }),
        "pass".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "bob" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();

    let (_dump_log, _dump_dashboard) = mm.mm_dump();
    log!({"Log path: {}", mm.log_path.display()});

    log!([block_on(enable_coins_eth_electrum(&mm, &["http://195.201.0.6:8565"]))]);

    check_too_low_volume_order_creation_fails(&mm, "MORTY", "ETH");
    check_too_low_volume_order_creation_fails(&mm, "ETH", "MORTY");
    check_too_low_volume_order_creation_fails(&mm, "JST", "MORTY");
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
// https://github.com/KomodoPlatform/atomicDEX-API/issues/473
fn setprice_min_volume_should_be_displayed_in_orderbook() {
    let bob_passphrase = get_passphrase(&".env.seed", "BOB_PASSPHRASE").unwrap();

    let coins = json! ([
        {"coin":"RICK","asset":"RICK","rpcport":8923,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","rpcport":11608,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"}},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}}}
    ]);

    let mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": bob_passphrase,
            "coins": coins,
            "rpc_password": "pass",
            "i_am_seed": true,
        }),
        "pass".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "bob" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();

    let (_dump_log, _dump_dashboard) = mm_bob.mm_dump();
    log!({"Bob log path: {}", mm_bob.log_path.display()});

    let mm_alice = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("ALICE_TRADE_IP") .ok(),
            "rpcip": env::var ("ALICE_TRADE_IP") .ok(),
            "passphrase": "alice passphrase",
            "coins": coins,
            "seednodes": [fomat!((mm_bob.ip))],
            "rpc_password": "pass",
        }),
        "pass".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "alice" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();

    let (_alice_dump_log, _alice_dump_dashboard) = mm_alice.mm_dump();
    log!({"Alice log path: {}", mm_alice.log_path.display()});

    log! ({"enable_coins (bob): {:?}", block_on (enable_coins_eth_electrum (&mm_bob, &["http://195.201.0.6:8565"]))});
    log! ({"enable_coins (alice): {:?}", block_on (enable_coins_eth_electrum (&mm_alice, &["http://195.201.0.6:8565"]))});

    // issue orderbook call on Alice side to trigger subscription to a topic
    block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "method": "orderbook",
        "base": "ETH",
        "rel": "JST",
    })))
    .unwrap();

    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "setprice",
        "base": "ETH",
        "rel": "JST",
        "price": "1",
        "volume": "10",
        "min_volume": "1",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!setprice: {}", rc.1);

    thread::sleep(Duration::from_secs(2));
    log!("Get ETH/JST orderbook on Bob side");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "orderbook",
        "base": "ETH",
        "rel": "JST",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!orderbook: {}", rc.1);

    let orderbook: Json = json::from_str(&rc.1).unwrap();
    log!("orderbook "[orderbook]);
    let asks = orderbook["asks"].as_array().unwrap();
    assert_eq!(asks.len(), 1, "Bob ETH/JST orderbook must have exactly 1 ask");

    let min_volume = asks[0]["min_volume"].as_str().unwrap();
    assert_eq!(min_volume, "1", "Bob ETH/JST ask must display correct min_volume");

    log!("Get ETH/JST orderbook on Alice side");
    let rc = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "method": "orderbook",
        "base": "ETH",
        "rel": "JST",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!orderbook: {}", rc.1);

    let orderbook: Json = json::from_str(&rc.1).unwrap();
    log!("orderbook "[orderbook]);
    let asks = orderbook["asks"].as_array().unwrap();
    assert_eq!(asks.len(), 1, "Alice ETH/JST orderbook must have exactly 1 ask");

    let min_volume = asks[0]["min_volume"].as_str().unwrap();
    assert_eq!(min_volume, "1", "Alice ETH/JST ask must display correct min_volume");
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
// https://github.com/KomodoPlatform/atomicDEX-API/issues/670
fn orderbook_should_work_without_coins_activation() {
    let bob_passphrase = get_passphrase(&".env.seed", "BOB_PASSPHRASE").unwrap();

    let coins = json! ([
        {"coin":"RICK","asset":"RICK","rpcport":8923,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","rpcport":11608,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"}},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}}}
    ]);

    let mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": bob_passphrase,
            "coins": coins,
            "rpc_password": "pass",
            "i_am_seed": true,
        }),
        "pass".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "bob" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();

    let (_dump_log, _dump_dashboard) = mm_bob.mm_dump();
    log!({"Bob log path: {}", mm_bob.log_path.display()});

    let mm_alice = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("ALICE_TRADE_IP") .ok(),
            "rpcip": env::var ("ALICE_TRADE_IP") .ok(),
            "passphrase": "alice passphrase",
            "coins": coins,
            "seednodes": [fomat!((mm_bob.ip))],
            "rpc_password": "pass",
        }),
        "pass".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "alice" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();

    let (_alice_dump_log, _alice_dump_dashboard) = mm_alice.mm_dump();
    log!({"Alice log path: {}", mm_alice.log_path.display()});

    log! ({"enable_coins (bob): {:?}", block_on (enable_coins_eth_electrum (&mm_bob, &["http://195.201.0.6:8565"]))});

    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "setprice",
        "base": "ETH",
        "rel": "JST",
        "price": "1",
        "volume": "10",
        "min_volume": "1",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!setprice: {}", rc.1);

    log!("Get ETH/JST orderbook on Alice side");
    let rc = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "method": "orderbook",
        "base": "ETH",
        "rel": "JST",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!orderbook: {}", rc.1);

    let orderbook: Json = json::from_str(&rc.1).unwrap();
    log!("orderbook "[orderbook]);
    let asks = orderbook["asks"].as_array().unwrap();
    assert_eq!(asks.len(), 1, "Alice ETH/JST orderbook must have exactly 1 ask");
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_fill_or_kill_taker_order_should_not_transform_to_maker() {
    let bob_passphrase = get_passphrase(&".env.client", "BOB_PASSPHRASE").unwrap();

    let coins = json! ([
        {"coin":"RICK","asset":"RICK","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"}},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}}}
    ]);

    let mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 8999,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": bob_passphrase,
            "coins": coins,
            "rpc_password": "password",
            "i_am_seed": true,
        }),
        "password".into(),
        local_start!("bob"),
    )
    .unwrap();

    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log! ({"Bob log path: {}", mm_bob.log_path.display()});
    log!([block_on(enable_coins_eth_electrum(&mm_bob, &[
        "http://195.201.0.6:8565"
    ]))]);

    log!("Issue bob ETH/JST sell request");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "sell",
        "base": "ETH",
        "rel": "JST",
        "price": 1,
        "volume": 0.1,
        "order_type": {
            "type": "FillOrKill"
        },
        "timeout": 2,
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!sell: {}", rc.1);
    let sell_json: Json = json::from_str(&rc.1).unwrap();
    let order_type = sell_json["result"]["order_type"]["type"].as_str();
    assert_eq!(order_type, Some("FillOrKill"));

    log!("Wait for 4 seconds for Bob order to be cancelled");
    thread::sleep(Duration::from_secs(4));

    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "my_orders",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!my_orders: {}", rc.1);
    let my_orders: Json = json::from_str(&rc.1).unwrap();
    let my_maker_orders: HashMap<String, Json> = json::from_value(my_orders["result"]["maker_orders"].clone()).unwrap();
    let my_taker_orders: HashMap<String, Json> = json::from_value(my_orders["result"]["taker_orders"].clone()).unwrap();
    assert!(my_maker_orders.is_empty(), "maker_orders must be empty");
    assert!(my_taker_orders.is_empty(), "taker_orders must be empty");
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_gtc_taker_order_should_transform_to_maker() {
    let bob_passphrase = get_passphrase(&".env.client", "BOB_PASSPHRASE").unwrap();

    let coins = json! ([
        {"coin":"RICK","asset":"RICK","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"}},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}}}
    ]);

    let mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 8999,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": bob_passphrase,
            "coins": coins,
            "rpc_password": "password",
            "i_am_seed": true,
        }),
        "password".into(),
        local_start!("bob"),
    )
    .unwrap();

    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log! ({"Bob log path: {}", mm_bob.log_path.display()});
    log!([block_on(enable_coins_eth_electrum(&mm_bob, &[
        "http://195.201.0.6:8565"
    ]))]);

    log!("Issue bob ETH/JST sell request");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "sell",
        "base": "ETH",
        "rel": "JST",
        "price": 1,
        "volume": 0.1,
        "order_type": {
            "type": "GoodTillCancelled"
        },
        "timeout": 2,
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!setprice: {}", rc.1);
    let rc_json: Json = json::from_str(&rc.1).unwrap();
    let uuid: Uuid = json::from_value(rc_json["result"]["uuid"].clone()).unwrap();

    log!("Wait for 4 seconds for Bob order to be converted to maker");
    thread::sleep(Duration::from_secs(4));

    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "my_orders",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!my_orders: {}", rc.1);
    let my_orders: Json = json::from_str(&rc.1).unwrap();
    let my_maker_orders: HashMap<String, Json> = json::from_value(my_orders["result"]["maker_orders"].clone()).unwrap();
    let my_taker_orders: HashMap<String, Json> = json::from_value(my_orders["result"]["taker_orders"].clone()).unwrap();
    assert_eq!(1, my_maker_orders.len(), "maker_orders must have exactly 1 order");
    assert!(my_taker_orders.is_empty(), "taker_orders must be empty");
    let order_path = mm_bob.folder.join(format!(
        "DB/{}/ORDERS/MY/MAKER/{}.json",
        hex::encode(rmd160_from_passphrase(&bob_passphrase)),
        uuid
    ));
    log!("Order path "(order_path.display()));
    assert!(order_path.exists());
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_set_price_must_save_order_to_db() {
    let bob_passphrase = get_passphrase(&".env.client", "BOB_PASSPHRASE").unwrap();

    let coins = json! ([
        {"coin":"RICK","asset":"RICK","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"}},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}}}
    ]);

    let mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 8999,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": bob_passphrase,
            "coins": coins,
            "rpc_password": "password",
            "i_am_seed": true,
        }),
        "password".into(),
        local_start!("bob"),
    )
    .unwrap();

    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log! ({"Bob log path: {}", mm_bob.log_path.display()});
    log!([block_on(enable_coins_eth_electrum(&mm_bob, &[
        "http://195.201.0.6:8565"
    ]))]);

    log!("Issue bob ETH/JST sell request");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "setprice",
        "base": "ETH",
        "rel": "JST",
        "price": 1,
        "volume": 0.1
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!setprice: {}", rc.1);
    let rc_json: Json = json::from_str(&rc.1).unwrap();
    let uuid: Uuid = json::from_value(rc_json["result"]["uuid"].clone()).unwrap();
    let order_path = mm_bob.folder.join(format!(
        "DB/{}/ORDERS/MY/MAKER/{}.json",
        hex::encode(rmd160_from_passphrase(&bob_passphrase)),
        uuid
    ));
    assert!(order_path.exists());
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_set_price_response_format() {
    let bob_passphrase = get_passphrase(&".env.client", "BOB_PASSPHRASE").unwrap();

    let coins = json! ([
        {"coin":"RICK","asset":"RICK","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"}},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}}}
    ]);

    let mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 8999,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": bob_passphrase,
            "coins": coins,
            "rpc_password": "password",
            "i_am_seed": true,
        }),
        "password".into(),
        local_start!("bob"),
    )
    .unwrap();

    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log! ({"Bob log path: {}", mm_bob.log_path.display()});
    log!([block_on(enable_coins_eth_electrum(&mm_bob, &[
        "http://195.201.0.6:8565"
    ]))]);

    log!("Issue bob ETH/JST sell request");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "setprice",
        "base": "ETH",
        "rel": "JST",
        "price": 1,
        "volume": 0.1
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!setprice: {}", rc.1);
    let rc_json: Json = json::from_str(&rc.1).unwrap();
    let _: BigDecimal = json::from_value(rc_json["result"]["max_base_vol"].clone()).unwrap();
    let _: BigDecimal = json::from_value(rc_json["result"]["min_base_vol"].clone()).unwrap();
    let _: BigDecimal = json::from_value(rc_json["result"]["price"].clone()).unwrap();

    let _: BigRational = json::from_value(rc_json["result"]["max_base_vol_rat"].clone()).unwrap();
    let _: BigRational = json::from_value(rc_json["result"]["min_base_vol_rat"].clone()).unwrap();
    let _: BigRational = json::from_value(rc_json["result"]["price_rat"].clone()).unwrap();
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
// https://github.com/KomodoPlatform/atomicDEX-API/issues/635
fn set_price_with_cancel_previous_should_broadcast_cancelled_message() {
    let coins = json!([
        {"coin":"RICK","asset":"RICK","rpcport":8923,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","rpcport":11608,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"},"rpcport":80},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0xc0eb7AeD740E1796992A08962c15661bDEB58003"}}}
    ]);

    // start bob and immediately place the order
    let mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": "bob passphrase",
            "coins": coins,
            "i_am_seed": true,
            "rpc_password": "pass",
        }),
        "pass".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "bob" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();
    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log!({"Bob log path: {}", mm_bob.log_path.display()});
    // Enable coins on Bob side. Print the replies in case we need the "address".
    log! ({"enable_coins (bob): {:?}", block_on (enable_coins_eth_electrum (&mm_bob, &["https://ropsten.infura.io/v3/c01c1b4cf66642528547624e1d6d9d6b"]))});

    let set_price_json = json! ({
        "userpass": mm_bob.userpass,
        "method": "setprice",
        "base": "RICK",
        "rel": "MORTY",
        "price": 0.9,
        "volume": "0.9",
    });
    log!("Issue sell request on Bob side by setting base/rel price…");
    let rc = block_on(mm_bob.rpc(set_price_json.clone())).unwrap();
    assert!(rc.0.is_success(), "!setprice: {}", rc.1);

    let mm_alice = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("ALICE_TRADE_IP") .ok(),
            "rpcip": env::var ("ALICE_TRADE_IP") .ok(),
            "passphrase": "alice passphrase",
            "coins": coins,
            "seednodes": [fomat!((mm_bob.ip))],
            "rpc_password": "pass",
        }),
        "pass".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "alice" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();

    let (_alice_dump_log, _alice_dump_dashboard) = mm_alice.mm_dump();
    log!({"Alice log path: {}", mm_alice.log_path.display()});

    // Enable coins on Alice side. Print the replies in case we need the "address".
    log! ({"enable_coins (alice): {:?}", block_on (enable_coins_eth_electrum (&mm_alice, &["https://ropsten.infura.io/v3/c01c1b4cf66642528547624e1d6d9d6b"]))});

    log!("Get RICK/MORTY orderbook on Alice side");
    let rc = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "method": "orderbook",
        "base": "RICK",
        "rel": "MORTY",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!orderbook: {}", rc.1);

    let alice_orderbook: Json = json::from_str(&rc.1).unwrap();
    log!("Alice orderbook "[alice_orderbook]);
    let asks = alice_orderbook["asks"].as_array().unwrap();
    assert_eq!(asks.len(), 1, "Alice RICK/MORTY orderbook must have exactly 1 ask");

    log!("Issue sell request again on Bob side by setting base/rel price…");
    let rc = block_on(mm_bob.rpc(set_price_json.clone())).unwrap();
    assert!(rc.0.is_success(), "!setprice: {}", rc.1);

    let pause = 2;
    log!("Waiting (" (pause) " seconds) for Bob to broadcast messages…");
    thread::sleep(Duration::from_secs(pause));

    // Bob orderbook must show 1 order
    log!("Get RICK/MORTY orderbook on Bob side");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "orderbook",
        "base": "RICK",
        "rel": "MORTY",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!orderbook: {}", rc.1);

    let bob_orderbook: Json = json::from_str(&rc.1).unwrap();
    log!("Bob orderbook "[bob_orderbook]);
    let asks = bob_orderbook["asks"].as_array().unwrap();
    assert_eq!(asks.len(), 1, "Bob RICK/MORTY orderbook must have exactly 1 ask");

    // Alice orderbook must have 1 order
    log!("Get RICK/MORTY orderbook on Alice side");
    let rc = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "method": "orderbook",
        "base": "RICK",
        "rel": "MORTY",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!orderbook: {}", rc.1);

    let alice_orderbook: Json = json::from_str(&rc.1).unwrap();
    log!("Alice orderbook "[alice_orderbook]);
    let asks = alice_orderbook["asks"].as_array().unwrap();
    assert_eq!(asks.len(), 1, "Alice RICK/MORTY orderbook must have exactly 1 ask");
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_batch_requests() {
    let coins = json!([
        {"coin":"RICK","asset":"RICK","rpcport":8923,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","rpcport":11608,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"},"rpcport":80},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0xc0eb7AeD740E1796992A08962c15661bDEB58003"}}}
    ]);

    // start bob and immediately place the order
    let mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": "bob passphrase",
            "coins": coins,
            "i_am_seed": true,
            "rpc_password": "pass",
        }),
        "pass".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "bob" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();
    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log!({"Bob log path: {}", mm_bob.log_path.display()});

    let batch_json = json!([
        {
            "userpass": mm_bob.userpass,
            "method": "electrum",
            "coin": "RICK",
            "servers": [{"url":"electrum1.cipig.net:10017"},{"url":"electrum2.cipig.net:10017"},{"url":"electrum3.cipig.net:10017"}],
            "mm2": 1,
        },
        {
            "userpass": mm_bob.userpass,
            "method": "electrum",
            "coin": "MORTY",
            "servers": [{"url":"electrum1.cipig.net:10018"},{"url":"electrum2.cipig.net:10018"},{"url":"electrum3.cipig.net:10018"}],
            "mm2": 1,
        },
        {
            "userpass": "error",
            "method": "electrum",
            "coin": "MORTY",
            "servers": [{"url":"electrum1.cipig.net:10018"},{"url":"electrum2.cipig.net:10018"},{"url":"electrum3.cipig.net:10018"}],
            "mm2": 1,
        },
    ]);

    let rc = block_on(mm_bob.rpc(batch_json)).unwrap();
    assert!(rc.0.is_success(), "!batch: {}", rc.1);
    log!((rc.1));
    let responses = json::from_str::<Vec<Json>>(&rc.1).unwrap();
    assert_eq!(responses[0]["coin"], Json::from("RICK"));
    assert_eq!(
        responses[0]["address"],
        Json::from("RRnMcSeKiLrNdbp91qNVQwwXx5azD4S4CD")
    );

    assert_eq!(responses[1]["coin"], Json::from("MORTY"));
    assert_eq!(
        responses[1]["address"],
        Json::from("RRnMcSeKiLrNdbp91qNVQwwXx5azD4S4CD")
    );

    assert!(responses[2]["error"].as_str().unwrap().contains("Userpass is invalid!"));
}

fn request_metrics(mm: &MarketMakerIt) -> MetricsJson {
    let (status, metrics, _headers) = block_on(mm.rpc(json!({ "method": "metrics"}))).unwrap();
    assert_eq!(status, StatusCode::OK, "RPC «metrics» failed with status «{}»", status);
    json::from_str(&metrics).unwrap()
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_metrics_method() {
    let coins = json!([
        {"coin":"RICK","asset":"RICK","rpcport":8923,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
    ]);

    let mm = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "passphrase": "face pin block number add byte put seek mime test note password sin tab multiple",
            "coins": coins,
            "i_am_seed": true,
            "rpc_password": "pass",
        }),
        "pass".into(),
        local_start!("bob"),
    )
    .unwrap();
    let (_dump_log, _dump_dashboard) = mm.mm_dump();
    log!({ "log path: {}", mm.log_path.display() });

    let _electrum = block_on(enable_electrum(&mm, "RICK", false, &[
        "electrum1.cipig.net:10017",
        "electrum2.cipig.net:10017",
        "electrum3.cipig.net:10017",
    ]));

    let metrics = request_metrics(&mm);
    assert!(!metrics.metrics.is_empty());

    log!("Received metrics:");
    log!([metrics]);

    find_metrics_in_json(metrics, "rpc_client.traffic.out", &[("coin", "RICK")])
        .expect(r#"Couldn't find a metric with key = "traffic.out" and label: coin = "RICK" in received json"#);
}

#[test]
#[ignore]
#[cfg(not(target_arch = "wasm32"))]
fn test_electrum_tx_history() {
    fn get_tx_history_request_count(mm: &MarketMakerIt) -> u64 {
        let metrics = request_metrics(&mm);
        match find_metrics_in_json(metrics, "tx.history.request.count", &[
            ("coin", "RICK"),
            ("method", "blockchain.scripthash.get_history"),
        ])
        .unwrap()
        {
            MetricType::Counter { value, .. } => value,
            _ => panic!("tx.history.request.count should be a counter"),
        }
    }

    let coins = json!([
        {"coin":"RICK","asset":"RICK","rpcport":8923,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
    ]);

    let mut mm = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "passphrase": "face pin block number add byte put seek mime test note password sin tab multiple",
            "coins": coins,
            "i_am_seed": true,
            "rpc_password": "pass",
            "metrics_interval": 30.
        }),
        "pass".into(),
        local_start!("bob"),
    )
    .unwrap();
    let (_dump_log, _dump_dashboard) = mm.mm_dump();
    log!({ "log path: {}", mm.log_path.display() });

    // Enable RICK electrum client with tx_history loop.
    let electrum = block_on(enable_electrum(&mm, "RICK", true, &[
        "electrum1.cipig.net:10017",
        "electrum2.cipig.net:10017",
        "electrum3.cipig.net:10017",
    ]));

    // Wait till tx_history will not be loaded
    block_on(mm.wait_for_log(500., |log| log.contains("history has been loaded successfully"))).unwrap();

    // tx_history is requested every 30 seconds, wait another iteration
    thread::sleep(Duration::from_secs(31));

    // Balance is not changed, therefore tx_history shouldn't be reloaded.
    // Request metrics and check if the MarketMaker has requested tx_history only once
    assert_eq!(get_tx_history_request_count(&mm), 1);

    // make a transaction to change balance
    let mut enable_res = HashMap::new();
    enable_res.insert("RICK", electrum);
    log!("enable_coins: "[enable_res]);
    withdraw_and_send(
        &mm,
        "RICK",
        "RRYmiZSDo3UdHHqj1rLKf8cbJroyv9NxXw",
        &enable_res,
        "-0.00001",
        0.001,
    );

    // Wait another iteration
    thread::sleep(Duration::from_secs(31));

    // tx_history should be reloaded on next loop iteration
    assert_eq!(get_tx_history_request_count(&mm), 2);
}

#[cfg(not(target_arch = "wasm32"))]
#[allow(dead_code)]
fn spin_n_nodes(seednodes: &[&str], coins: &Json, n: usize) -> Vec<(MarketMakerIt, RaiiDump, RaiiDump)> {
    let mut mm_nodes = Vec::with_capacity(n);
    for i in 0..n {
        let mut mm = MarketMakerIt::start(
            json! ({
                "gui": "nogui",
                "netid": 9998,
                "myipaddr": env::var ("ALICE_TRADE_IP") .ok(),
                "rpcip": env::var ("ALICE_TRADE_IP") .ok(),
                "passphrase": format!("alice passphrase {}", i),
                "coins": coins,
                "seednodes": seednodes,
                "rpc_password": "pass",
            }),
            "pass".into(),
            local_start!("alice"),
        )
        .unwrap();

        let (alice_dump_log, alice_dump_dashboard) = mm.mm_dump();
        log!({ "Alice {} log path: {}", i, mm.log_path.display() });
        for seednode in seednodes.iter() {
            block_on(mm.wait_for_log(22., |log| log.contains(&format!("Dialed {}", seednode)))).unwrap();
        }
        mm_nodes.push((mm, alice_dump_log, alice_dump_dashboard));
    }
    mm_nodes
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_withdraw_cashaddresses() {
    let coins = json!([
        {"coin":"BCH","pubtype":0,"p2shtype":5,"mm2":1,"fork_id": "0x40","protocol":{"type":"UTXO"},
         "address_format":{"format":"cashaddress","network":"bchtest"}},
    ]);

    let mm = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "passphrase": "face pin lock number add byte put seek mime test note password sin tab multiple",
            "coins": coins,
            "i_am_seed": true,
            "rpc_password": "pass",
        }),
        "pass".into(),
        local_start!("bob"),
    )
    .unwrap();
    let (_dump_log, _dump_dashboard) = mm.mm_dump();
    log!({ "log path: {}", mm.log_path.display() });

    let electrum = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "electrum",
        "coin": "BCH",
        "servers": [{"url":"blackie.c3-soft.com:60001"}, {"url":"bch0.kister.net:51001"}, {"url":"testnet.imaginary.cash:50001"}],
        "mm2": 1,
    })))
    .unwrap();

    assert_eq!(
        electrum.0,
        StatusCode::OK,
        "RPC «electrum» failed with {} {}",
        electrum.0,
        electrum.1
    );
    let electrum: Json = json::from_str(&electrum.1).unwrap();
    log!([electrum]);

    // make withdraw from cashaddress to cashaddress
    let withdraw = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "withdraw",
        "coin": "BCH",
        "to": "bchtest:qr39na5d25wdeecgw3euh9fkd4ygvd4pnsury96597",
        "amount": 0.00001,
    })))
    .unwrap();

    assert!(withdraw.0.is_success(), "BCH withdraw: {}", withdraw.1);
    let withdraw_json: Json = json::from_str(&withdraw.1).unwrap();
    log!((withdraw_json));

    // check "from" addresses
    let from: Vec<&str> = withdraw_json["from"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(from, vec!["bchtest:qqgp9xh3435xamv7ghct8emer2s2erzj8gx3gnhwkq"]);

    // check "to" addresses
    let to: Vec<&str> = withdraw_json["to"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(to, vec!["bchtest:qr39na5d25wdeecgw3euh9fkd4ygvd4pnsury96597"]);

    // send the transaction
    let send_tx = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "send_raw_transaction",
        "coin": "BCH",
        "tx_hex": withdraw_json["tx_hex"],
    })))
    .unwrap();
    assert!(send_tx.0.is_success(), "BCH send_raw_transaction: {}", send_tx.1);
    log!((send_tx.1));

    // Wait 5 seconds to avoid double spending
    thread::sleep(Duration::from_secs(5));

    // make withdraw from cashaddress to legacy
    let withdraw = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "withdraw",
        "coin": "BCH",
        "to": "1WxswvLF2HdaDr4k77e92VjaXuPQA8Uji",
        "amount": 0.00001,
    })))
    .unwrap();

    assert!(withdraw.0.is_success(), "BCH withdraw: {}", withdraw.1);
    let withdraw_json: Json = json::from_str(&withdraw.1).unwrap();
    log!((withdraw_json));

    // check "from" addresses
    let from: Vec<&str> = withdraw_json["from"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(from, vec!["bchtest:qqgp9xh3435xamv7ghct8emer2s2erzj8gx3gnhwkq"]);

    // check "to" addresses
    let to: Vec<&str> = withdraw_json["to"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(to, vec!["1WxswvLF2HdaDr4k77e92VjaXuPQA8Uji"]);

    // send the transaction
    let send_tx = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "send_raw_transaction",
        "coin": "BCH",
        "tx_hex": withdraw_json["tx_hex"],
    })))
    .unwrap();
    assert!(send_tx.0.is_success(), "BCH send_raw_transaction: {}", send_tx.1);
    log!((send_tx.1));

    // Wait 5 seconds to avoid double spending
    thread::sleep(Duration::from_secs(5));

    //Disable BCH to enable in Legacy Mode
    let rc = block_on(mm.rpc(json!({
        "userpass": mm.userpass,
        "method": "disable_coin",
        "coin": "BCH",
    })))
    .unwrap();
    assert_eq!(rc.0, StatusCode::OK, "RPC «disable_coin» failed with status «{}»", rc.0);

    let electrum = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "electrum",
        "coin": "BCH",
        "servers": [{"url":"blackie.c3-soft.com:60001"}, {"url":"bch0.kister.net:51001"}, {"url":"testnet.imaginary.cash:50001"}],
        "address_format":{"format":"standard"},
        "mm2": 1,
    })))
    .unwrap();

    assert_eq!(
        electrum.0,
        StatusCode::OK,
        "RPC «electrum» failed with {} {}",
        electrum.0,
        electrum.1
    );
    let electrum: Json = json::from_str(&electrum.1).unwrap();
    log!([electrum]);

    // make withdraw from Legacy to Cashaddress
    let withdraw = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "withdraw",
        "coin": "BCH",
        "to": "bchtest:qr39na5d25wdeecgw3euh9fkd4ygvd4pnsury96597",
        "amount": 0.00001,
    })))
    .unwrap();

    assert!(withdraw.0.is_success(), "BCH withdraw: {}", withdraw.1);
    let withdraw_json: Json = json::from_str(&withdraw.1).unwrap();
    log!((withdraw_json));

    // check "from" addresses
    let from: Vec<&str> = withdraw_json["from"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(from, vec!["12Tz6nWqA7e5tV7m6d1EzMkNs6MQVW4UMw"]);

    // check "to" addresses
    let to: Vec<&str> = withdraw_json["to"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(to, vec!["bchtest:qr39na5d25wdeecgw3euh9fkd4ygvd4pnsury96597"]);

    // send the transaction
    let send_tx = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "send_raw_transaction",
        "coin": "BCH",
        "tx_hex": withdraw_json["tx_hex"],
    })))
    .unwrap();
    assert!(send_tx.0.is_success(), "BCH send_raw_transaction: {}", send_tx.1);
    log!((send_tx.1));
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_withdraw_to_different_cashaddress_network_should_fail() {
    let coins = json!([
        {"coin":"BCH","pubtype":0,"p2shtype":5,"mm2":1,"fork_id": "0x40","protocol":{"type":"UTXO"},
         "address_format":{"format":"cashaddress","network":"bchtest"}},
    ]);

    let mm = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "passphrase": "face pin lock number add byte put seek mime test note password sin tab multiple",
            "coins": coins,
            "i_am_seed": true,
            "rpc_password": "pass",
        }),
        "pass".into(),
        local_start!("bob"),
    )
    .unwrap();
    let (_dump_log, _dump_dashboard) = mm.mm_dump();
    log!({ "log path: {}", mm.log_path.display() });

    let electrum = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "electrum",
        "coin": "BCH",
        "servers": [{"url":"blackie.c3-soft.com:60001"}, {"url":"bch0.kister.net:51001"}, {"url":"testnet.imaginary.cash:50001"}],
        "mm2": 1,
    })))
    .unwrap();

    assert_eq!(
        electrum.0,
        StatusCode::OK,
        "RPC «electrum» failed with {} {}",
        electrum.0,
        electrum.1
    );
    let electrum: Json = json::from_str(&electrum.1).unwrap();
    log!([electrum]);

    // make withdraw to from bchtest to bitcoincash should fail
    let withdraw = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "withdraw",
        "coin": "BCH",
        "to": "bitcoincash:qqyf96yqdrpa8f6pkf9f00ap068m5tgvly28qsfq9p",
        "amount": 0.00001,
    })))
    .unwrap();

    assert!(withdraw.0.is_server_error(), "BCH withdraw: {}", withdraw.1);
    log!([withdraw.1]);

    block_on(mm.stop()).unwrap();
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_common_cashaddresses() {
    let coins = json!([
        {"coin":"BCH","pubtype":0,"p2shtype":5,"mm2":1,"protocol":{"type":"UTXO"},
         "address_format":{"format":"cashaddress","network":"bchtest"}},
    ]);

    let mm = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "passphrase": "face pin block number add byte put seek mime test note password sin tab multiple",
            "coins": coins,
            "i_am_seed": true,
            "rpc_password": "pass",
        }),
        "pass".into(),
        local_start!("bob"),
    )
    .unwrap();
    let (_dump_log, _dump_dashboard) = mm.mm_dump();
    log!({ "log path: {}", mm.log_path.display() });

    // Enable BCH electrum client with tx_history loop.
    // Enable RICK electrum client with tx_history loop.
    let electrum = block_on(mm.rpc (json! ({
        "userpass": mm.userpass,
        "method": "electrum",
        "coin": "BCH",
        "servers": [{"url":"blackie.c3-soft.com:60001"}, {"url":"bch0.kister.net:51001"}, {"url":"testnet.imaginary.cash:50001"}],
        "mm2": 1,
    }))).unwrap();

    assert_eq!(
        electrum.0,
        StatusCode::OK,
        "RPC «electrum» failed with {} {}",
        electrum.0,
        electrum.1
    );
    let electrum: Json = json::from_str(&electrum.1).unwrap();
    log!([electrum]);

    assert_eq!(
        electrum["address"].as_str().unwrap(),
        "bchtest:qze8g4gx3z428jjcxzpycpxl7ke7d947gca2a7n2la"
    );

    // check my_balance
    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "my_balance",
        "coin": "BCH",
    })))
    .unwrap();
    assert_eq!(rc.0, StatusCode::OK, "RPC «my_balance» failed with status «{}»", rc.0);
    let json: Json = json::from_str(&rc.1).unwrap();
    let my_balance_address = json["address"].as_str().unwrap();
    assert_eq!(my_balance_address, "bchtest:qze8g4gx3z428jjcxzpycpxl7ke7d947gca2a7n2la");

    // check get_enabled_coins
    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "get_enabled_coins",
    })))
    .unwrap();
    assert_eq!(
        rc.0,
        StatusCode::OK,
        "RPC «get_enabled_coins» failed with status «{}»",
        rc.0
    );
    let json: Json = json::from_str(&rc.1).unwrap();

    let obj = &json["result"].as_array().unwrap()[0];
    assert_eq!(obj["ticker"].as_str().unwrap(), "BCH");
    assert_eq!(
        obj["address"].as_str().unwrap(),
        "bchtest:qze8g4gx3z428jjcxzpycpxl7ke7d947gca2a7n2la"
    );
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_convert_utxo_address() {
    let coins = json!([
        {"coin":"BCH","pubtype":0,"p2shtype":5,"mm2":1,"fork_id": "0x40","protocol":{"type":"UTXO"},
         "address_format":{"format":"cashaddress","network":"bitcoincash"}},
    ]);

    let mm = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "passphrase": "face pin block number add byte put seek mime test note password sin tab multiple",
            "coins": coins,
            "i_am_seed": true,
            "rpc_password": "pass",
        }),
        "pass".into(),
        local_start!("bob"),
    )
    .unwrap();
    let (_dump_log, _dump_dashboard) = mm.mm_dump();
    log!({ "log path: {}", mm.log_path.display() });

    let _electrum = block_on(enable_electrum(&mm, "BCH", false, &[
        "blackie.c3-soft.com:60001",
        "bch0.kister.net:51001",
        "testnet.imaginary.cash:50001",
    ]));

    // test standard to cashaddress
    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "convertaddress",
        "coin": "BCH",
        "from": "1DmFp16U73RrVZtYUbo2Ectt8mAnYScpqM",
        "to_address_format":{"format":"cashaddress","network":"bitcoincash"},
    })))
    .unwrap();
    assert_eq!(
        rc.0,
        StatusCode::OK,
        "RPC «convertaddress» failed with status «{}»",
        rc.0
    );
    let actual: Json = json::from_str(&rc.1).unwrap();

    let expected = json!({
        "result": {
            "address": "bitcoincash:qzxqqt9lh4feptf0mplnk58gnajfepzwcq9f2rxk55",
        },
    });
    assert_eq!(actual, expected);

    // test cashaddress to standard
    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "convertaddress",
        "coin": "BCH",
        "from": "bitcoincash:qzxqqt9lh4feptf0mplnk58gnajfepzwcq9f2rxk55",
        "to_address_format":{"format":"standard"},
    })))
    .unwrap();
    assert_eq!(
        rc.0,
        StatusCode::OK,
        "RPC «convertaddress» failed with status «{}»",
        rc.0
    );
    let actual: Json = json::from_str(&rc.1).unwrap();

    let expected = json!({
        "result": {
            "address": "1DmFp16U73RrVZtYUbo2Ectt8mAnYScpqM",
        },
    });
    assert_eq!(actual, expected);

    // test standard to standard
    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "convertaddress",
        "coin": "BCH",
        "from": "1DmFp16U73RrVZtYUbo2Ectt8mAnYScpqM",
        "to_address_format":{"format":"standard"},
    })))
    .unwrap();
    assert_eq!(
        rc.0,
        StatusCode::OK,
        "RPC «convertaddress» failed with status «{}»",
        rc.0
    );
    let actual: Json = json::from_str(&rc.1).unwrap();

    let expected = json!({
        "result": {
            "address": "1DmFp16U73RrVZtYUbo2Ectt8mAnYScpqM",
        },
    });
    assert_eq!(actual, expected);

    // test invalid address
    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "convertaddress",
        "coin": "BCH",
        "from": "0000000000000000000000000000000000",
        "to_address_format":{"format":"standard"},
    })))
    .unwrap();
    assert!(
        rc.0.is_server_error(),
        "!convertaddress success but should be error: {}",
        rc.1
    );
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_convert_segwit_address() {
    let coins = json! ([
        {
            "coin": "tBTC",
            "name": "tbitcoin",
            "fname": "tBitcoin",
            "rpcport": 18332,
            "pubtype": 111,
            "p2shtype": 196,
            "wiftype": 239,
            "segwit": true,
            "bech32_hrp": "tb",
            "txfee": 1000,
            "mm2": 1,
            "required_confirmations": 0,
            "protocol": {
                "type": "UTXO"
            }
        }
    ]);

    let mm = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "passphrase": "face pin block number add byte put seek mime test note password sin tab multiple",
            "coins": coins,
            "i_am_seed": true,
            "rpc_password": "pass",
        }),
        "pass".into(),
        local_start!("bob"),
    )
    .unwrap();
    let (_dump_log, _dump_dashboard) = mm.mm_dump();
    log!({ "log path: {}", mm.log_path.display() });

    let _electrum = block_on(enable_electrum(&mm, "tBTC", false, &[
        "electrum1.cipig.net:10068",
        "electrum2.cipig.net:10068",
        "electrum3.cipig.net:10068",
    ]));

    // test standard to segwit
    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "convertaddress",
        "coin": "tBTC",
        "from": "mqWYEGxLeK843n3xMTe8EWTFPyoSZjtUXb",
        "to_address_format":{"format":"segwit"},
    })))
    .unwrap();
    assert_eq!(
        rc.0,
        StatusCode::OK,
        "RPC «convertaddress» failed with status «{}»",
        rc.0
    );
    let actual: Json = json::from_str(&rc.1).unwrap();

    let expected = json!({
        "result": {
            "address": "tb1qdkwjk42dw6pryvs9sl0ht3pn3mxghuma64jst5",
        },
    });
    assert_eq!(actual, expected);

    // test segwit to standard
    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "convertaddress",
        "coin": "tBTC",
        "from": "tb1qdkwjk42dw6pryvs9sl0ht3pn3mxghuma64jst5",
        "to_address_format":{"format":"standard"},
    })))
    .unwrap();
    assert_eq!(
        rc.0,
        StatusCode::OK,
        "RPC «convertaddress» failed with status «{}»",
        rc.0
    );
    let actual: Json = json::from_str(&rc.1).unwrap();

    let expected = json!({
        "result": {
            "address": "mqWYEGxLeK843n3xMTe8EWTFPyoSZjtUXb",
        },
    });
    assert_eq!(actual, expected);

    // test invalid tBTC standard address
    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "convertaddress",
        "coin": "tBTC",
        "from": "1AzawDsMqHgoGfaLdtfkQbEvXzCjk5oyFx",
        "to_address_format":{"format":"segwit"},
    })))
    .unwrap();
    assert!(
        rc.0.is_server_error(),
        "!convertaddress success but should be error: {}",
        rc.1
    );
    assert!(rc.1.contains("Expected a valid P2PKH or P2SH prefix for tBTC"));

    // test invalid tBTC segwit address
    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "convertaddress",
        "coin": "tBTC",
        "from": "ltc1qdkwjk42dw6pryvs9sl0ht3pn3mxghuma64jst5",
        "to_address_format":{"format":"standard"},
    })))
    .unwrap();
    assert!(
        rc.0.is_server_error(),
        "!convertaddress success but should be error: {}",
        rc.1
    );
    assert!(rc
        .1
        .contains("Invalid address: ltc1qdkwjk42dw6pryvs9sl0ht3pn3mxghuma64jst5"));
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_convert_eth_address() {
    let coins = json!([
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"}},
    ]);

    // start mm and immediately place the order
    let mm = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": "bob passphrase",
            "coins": coins,
            "i_am_seed": true,
            "rpc_password": "pass",
        }),
        "pass".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "bob" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();
    let (_dump_log, _dump_dashboard) = mm.mm_dump();
    log!({ "log path: {}", mm.log_path.display() });

    block_on(enable_native(&mm, "ETH", &["http://195.201.0.6:8565"]));

    // test single-case to mixed-case
    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "convertaddress",
        "coin": "ETH",
        "from": "0xfb6916095ca1df60bb79ce92ce3ea74c37c5d359",
        "to_address_format":{"format":"mixedcase"},
    })))
    .unwrap();
    assert_eq!(
        rc.0,
        StatusCode::OK,
        "RPC «convertaddress» failed with status «{}»",
        rc.0
    );
    let actual: Json = json::from_str(&rc.1).unwrap();

    let expected = json!({
        "result": {
            "address": "0xfB6916095ca1df60bB79Ce92cE3Ea74c37c5d359",
        },
    });
    assert_eq!(actual, expected);

    // test mixed-case to mixed-case (expect error)
    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "convertaddress",
        "coin": "ETH",
        "from": "0xfB6916095ca1df60bB79Ce92cE3Ea74c37c5d359",
        "to_address_format":{"format":"mixedcase"},
    })))
    .unwrap();
    assert_eq!(
        rc.0,
        StatusCode::OK,
        "RPC «convertaddress» failed with status «{}»",
        rc.0
    );
    let actual: Json = json::from_str(&rc.1).unwrap();

    let expected = json!({
        "result": {
            "address": "0xfB6916095ca1df60bB79Ce92cE3Ea74c37c5d359",
        },
    });
    assert_eq!(actual, expected);

    // test invalid address
    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "convertaddress",
        "coin": "ETH",
        "from": "fB6916095ca1df60bB79Ce92cE3Ea74c37c5d359",
        "to_address_format":{"format":"mixedcase"},
    })))
    .unwrap();
    assert!(
        rc.0.is_server_error(),
        "!convertaddress success but should be error: {}",
        rc.1
    );
    assert!(rc.1.contains("Address must be prefixed with 0x"));
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_convert_qrc20_address() {
    let passphrase = "cV463HpebE2djP9ugJry5wZ9st5cc6AbkHXGryZVPXMH1XJK8cVU";
    let coins = json! ([
        {"coin":"QRC20","required_confirmations":0,"pubtype": 120,"p2shtype": 50,"wiftype": 128,"segwit": true,"txfee": 0,"mm2": 1,"mature_confirmations":2000,
         "protocol":{"type":"QRC20","protocol_data":{"platform":"QTUM","contract_address":"0xd362e096e873eb7907e205fadc6175c6fec7bc44"}}},
    ]);

    let mm = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 8999,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": passphrase,
            "coins": coins,
            "rpc_password": "password",
            "i_am_seed": true,
        }),
        "password".into(),
        local_start!("bob"),
    )
    .unwrap();

    let (_bob_dump_log, _bob_dump_dashboard) = mm.mm_dump();
    log! ({"Bob log path: {}", mm.log_path.display()});
    let _electrum = block_on(enable_qrc20(
        &mm,
        "QRC20",
        &["95.217.83.126:10001"],
        "0xba8b71f3544b93e2f681f996da519a98ace0107a",
    ));

    // test wallet to contract
    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "convertaddress",
        "coin": "QRC20",
        "from": "qKVvtDqpnFGDxsDzck5jmLwdnD2jRH6aM8",
        "to_address_format":{"format":"contract"},
    })))
    .unwrap();
    assert_eq!(
        rc.0,
        StatusCode::OK,
        "RPC «convertaddress» failed with status «{}»",
        rc.0
    );
    let actual: Json = json::from_str(&rc.1).unwrap();

    let expected = json!({
        "result": {
            "address": "0x1549128bbfb33b997949b4105b6a6371c998e212",
        },
    });
    assert_eq!(actual, expected);

    // test contract to wallet
    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "convertaddress",
        "coin": "QRC20",
        "from": "0x1549128bbfb33b997949b4105b6a6371c998e212",
        "to_address_format":{"format":"wallet"},
    })))
    .unwrap();
    assert_eq!(
        rc.0,
        StatusCode::OK,
        "RPC «convertaddress» failed with status «{}»",
        rc.0
    );
    let actual: Json = json::from_str(&rc.1).unwrap();

    let expected = json!({
        "result": {
            "address": "qKVvtDqpnFGDxsDzck5jmLwdnD2jRH6aM8",
        },
    });
    assert_eq!(actual, expected);

    // test wallet to wallet
    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "convertaddress",
        "coin": "QRC20",
        "from": "qKVvtDqpnFGDxsDzck5jmLwdnD2jRH6aM8",
        "to_address_format":{"format":"wallet"},
    })))
    .unwrap();
    assert_eq!(
        rc.0,
        StatusCode::OK,
        "RPC «convertaddress» failed with status «{}»",
        rc.0
    );
    let actual: Json = json::from_str(&rc.1).unwrap();

    let expected = json!({
        "result": {
            "address": "qKVvtDqpnFGDxsDzck5jmLwdnD2jRH6aM8",
        },
    });
    assert_eq!(actual, expected);

    // test invalid address (invalid prefixes)
    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "convertaddress",
        "coin": "QRC20",
        "from": "RRnMcSeKiLrNdbp91qNVQwwXx5azD4S4CD",
        "to_address_format":{"format":"contract"},
    })))
    .unwrap();
    assert!(
        rc.0.is_server_error(),
        "!convertaddress success but should be error: {}",
        rc.1
    );
    log!((rc.1));
    assert!(rc.1.contains("Address has invalid prefixes"));

    // test invalid address
    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "convertaddress",
        "coin": "QRC20",
        "from": "0000000000000000000000000000000000",
        "to_address_format":{"format":"wallet"},
    })))
    .unwrap();
    assert!(
        rc.0.is_server_error(),
        "!convertaddress success but should be error: {}",
        rc.1
    );
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_switch_legacy_to_segwit_address() {
    let seed = "valley embody about obey never adapt gesture trust screen tube glide bread";

    let coins = json! ([
        {
            "coin": "tBTC",
            "name": "tbitcoin",
            "fname": "tBitcoin",
            "rpcport": 18332,
            "pubtype": 111,
            "p2shtype": 196,
            "wiftype": 239,
            "segwit": true,
            "bech32_hrp": "tb",
            "txfee": 0,
            "estimate_fee_mode": "ECONOMICAL",
            "mm2": 1,
            "required_confirmations": 0,
            "protocol": {
                "type": "UTXO"
            }
        }
    ]);

    let mm = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "passphrase": seed.to_string(),
            "coins": coins,
            "i_am_seed": true,
            "rpc_password": "pass",
        }),
        "pass".into(),
        local_start!("bob"),
    )
    .unwrap();
    let (_dump_log, _dump_dashboard) = mm.mm_dump();
    log!({ "log path: {}", mm.log_path.display() });

    //Enable tBTC in Legacy Mode
    let electrum = block_on(mm.rpc(json!({
        "userpass": mm.userpass,
        "method": "electrum",
        "coin": "tBTC",
        "servers": [{"url":"electrum1.cipig.net:10068"},{"url":"electrum2.cipig.net:10068"},{"url":"electrum3.cipig.net:10068"}],
        "mm2": 1,
    }))).unwrap();
    assert_eq!(
        electrum.0,
        StatusCode::OK,
        "RPC «electrum» failed with {} {}",
        electrum.0,
        electrum.1
    );
    let json: Json = json::from_str(&electrum.1).unwrap();
    let my_address = json["address"].as_str().unwrap();
    assert_eq!(my_address, "msZK9EuTh7ouM4kJ6jkRuCrDSABq9nER97");

    //Disable tBTC to enable in Segwit Mode
    let rc = block_on(mm.rpc(json!({
        "userpass": mm.userpass,
        "method": "disable_coin",
        "coin": "tBTC",
    })))
    .unwrap();
    assert_eq!(rc.0, StatusCode::OK, "RPC «disable_coin» failed with status «{}»", rc.0);

    //Enable tBTC in Segwit Mode
    let electrum = block_on(mm.rpc(json!({
        "userpass": mm.userpass,
        "method": "electrum",
        "coin": "tBTC",
        "servers": [{"url":"electrum1.cipig.net:10068"},{"url":"electrum2.cipig.net:10068"},{"url":"electrum3.cipig.net:10068"}],
        "mm2": 1,
        "address_format": {
            "format": "segwit",
        },
    }))).unwrap();
    assert_eq!(
        electrum.0,
        StatusCode::OK,
        "RPC «electrum» failed with {} {}",
        electrum.0,
        electrum.1
    );
    let json: Json = json::from_str(&electrum.1).unwrap();
    let my_address = json["address"].as_str().unwrap();
    assert_eq!(my_address, "tb1qssfmay8nnghx7ynlznejnjxn6m4pemz9v7fsxy");
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_validateaddress() {
    let coins = json!([
        {"coin":"RICK","asset":"RICK","rpcport":8923,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","rpcport":11608,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"}},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}}}
    ]);

    let (bob_file_passphrase, _bob_file_userpass) = from_env_file(slurp(&".env.seed").unwrap());
    let bob_passphrase = var("BOB_PASSPHRASE")
        .ok()
        .or(bob_file_passphrase)
        .expect("No BOB_PASSPHRASE or .env.seed/PASSPHRASE");

    let mm = MarketMakerIt::start(
        json!({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var("BOB_TRADE_IP").ok(),
            "rpcip": env::var("BOB_TRADE_IP").ok(),
            "canbind": env::var("BOB_TRADE_PORT").ok().map(|s| s.parse::<i64>().unwrap()),
            "passphrase": bob_passphrase,
            "coins": coins,
            "rpc_password": "pass",
            "i_am_seed": true,
        }),
        "pass".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "bob" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();
    let (_dump_log, _dump_dashboard) = mm.mm_dump();
    log!({"Log path: {}", mm.log_path.display()});
    log!([block_on(enable_coins_eth_electrum(&mm, &["http://195.201.0.6:8565"]))]);

    // test valid RICK address

    let rc = block_on(mm.rpc(json!({
        "userpass": mm.userpass,
        "method": "validateaddress",
        "coin": "RICK",
        "address": "RRnMcSeKiLrNdbp91qNVQwwXx5azD4S4CD",
    })))
    .unwrap();
    assert_eq!(
        rc.0,
        StatusCode::OK,
        "RPC «validateaddress» failed with status «{}»",
        rc.0
    );
    let actual: Json = json::from_str(&rc.1).unwrap();

    let expected = json!({
        "result": {
            "is_valid": true,
        },
    });
    assert_eq!(actual, expected);

    // test valid ETH address

    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "validateaddress",
        "coin": "ETH",
        "address": "0x7Bc1bBDD6A0a722fC9bffC49c921B685ECB84b94",
    })))
    .unwrap();
    assert_eq!(
        rc.0,
        StatusCode::OK,
        "RPC «validateaddress» failed with status «{}»",
        rc.0
    );
    let actual: Json = json::from_str(&rc.1).unwrap();

    let expected = json!({
        "result": {
            "is_valid": true,
        },
    });
    assert_eq!(actual, expected);

    // test invalid RICK address (legacy address format activated)

    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "validateaddress",
        "coin": "RICK",
        "address": "bchtest:qr39na5d25wdeecgw3euh9fkd4ygvd4pnsury96597",
    })))
    .unwrap();
    assert_eq!(
        rc.0,
        StatusCode::OK,
        "RPC «validateaddress» failed with status «{}»",
        rc.0
    );
    let json: Json = json::from_str(&rc.1).unwrap();
    let result = &json["result"];

    assert!(!result["is_valid"].as_bool().unwrap());
    let reason = result["reason"].as_str().unwrap();
    log!((reason));
    assert!(reason.contains("Legacy address format activated for RICK, but CashAddress format used instead"));

    // test invalid RICK address (invalid prefixes)

    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "validateaddress",
        "coin": "RICK",
        "address": "1DmFp16U73RrVZtYUbo2Ectt8mAnYScpqM",
    })))
    .unwrap();
    assert_eq!(
        rc.0,
        StatusCode::OK,
        "RPC «validateaddress» failed with status «{}»",
        rc.0
    );

    let json: Json = json::from_str(&rc.1).unwrap();
    let result = &json["result"];

    assert!(!result["is_valid"].as_bool().unwrap());
    let reason = result["reason"].as_str().unwrap();
    log!((reason));
    assert!(reason.contains("Expected a valid P2PKH or P2SH prefix"));

    // test invalid ETH address

    let rc = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "validateaddress",
        "coin": "ETH",
        "address": "7Bc1bBDD6A0a722fC9bffC49c921B685ECB84b94",
    })))
    .unwrap();
    assert_eq!(
        rc.0,
        StatusCode::OK,
        "RPC «validateaddress» failed with status «{}»",
        rc.0
    );
    let json: Json = json::from_str(&rc.1).unwrap();
    let result = &json["result"];

    assert!(!result["is_valid"].as_bool().unwrap());
    let reason = result["reason"].as_str().unwrap();
    log!((reason));
    assert!(reason.contains("Address must be prefixed with 0x"));
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_validateaddress_segwit() {
    let seed = "spice describe gravity federal blast come thank unfair canal monkey style afraid";

    let coins = json! ([
        {
            "coin": "tBTC",
            "name": "tbitcoin",
            "fname": "tBitcoin",
            "rpcport": 18332,
            "pubtype": 111,
            "p2shtype": 196,
            "wiftype": 239,
            "segwit": true,
            "bech32_hrp": "tb",
            "txfee": 1000,
            "mm2": 1,
            "required_confirmations": 0,
            "protocol": {
                "type": "UTXO"
            }
        }
    ]);

    let mm_alice = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 8100,
            "myipaddr": env::var ("ALICE_TRADE_IP") .ok(),
            "rpcip": env::var ("ALICE_TRADE_IP") .ok(),
            "passphrase": seed.to_string(),
            "coins": coins,
            "rpc_password": "password",
            "i_am_seed": true,
        }),
        "password".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "alice" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();

    let (_alice_dump_log, _alice_dump_dashboard) = mm_alice.mm_dump();
    log! ({"Alice log path: {}", mm_alice.log_path.display()});

    // wait until RPC API is active

    // Enable coins. Print the replies in case we need the address.
    let electrum = block_on(mm_alice.rpc(json!({
        "userpass": mm_alice.userpass,
        "method": "electrum",
        "coin": "tBTC",
        "servers": [{"url":"electrum1.cipig.net:10068"},{"url":"electrum2.cipig.net:10068"},{"url":"electrum3.cipig.net:10068"}],
        "mm2": 1,
        "address_format": {
            "format": "segwit",
        },
    }))).unwrap();
    assert_eq!(
        electrum.0,
        StatusCode::OK,
        "RPC «electrum» failed with {} {}",
        electrum.0,
        electrum.1
    );
    log!("enable_coins (alice): "[electrum]);

    let electrum_response: EnableElectrumResponse =
        json::from_str(&electrum.1).expect("Expected 'EnableElectrumResponse'");
    let mut enable_res = HashMap::new();
    enable_res.insert("tBTC", electrum_response);

    // test valid Segwit address
    let rc = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "method": "validateaddress",
        "coin": "tBTC",
        "address": "tb1qdkwjk42dw6pryvs9sl0ht3pn3mxghuma64jst5",
    })))
    .unwrap();
    assert_eq!(
        rc.0,
        StatusCode::OK,
        "RPC «validateaddress» failed with status «{}»",
        rc.0
    );
    let actual: Json = json::from_str(&rc.1).unwrap();

    let expected = json!({
        "result": {
            "is_valid": true,
        },
    });
    assert_eq!(actual, expected);

    // test invalid tBTC Segwit address (invalid hrp)
    let rc = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "method": "validateaddress",
        "coin": "tBTC",
        "address": "bc1qdkwjk42dw6pryvs9sl0ht3pn3mxghuma64jst5",
    })))
    .unwrap();
    assert_eq!(
        rc.0,
        StatusCode::OK,
        "RPC «validateaddress» failed with status «{}»",
        rc.0
    );

    let json: Json = json::from_str(&rc.1).unwrap();
    let result = &json["result"];

    assert!(!result["is_valid"].as_bool().unwrap());
    let reason = result["reason"].as_str().unwrap();
    log!((reason));
    assert!(reason.contains("Invalid address: bc1qdkwjk42dw6pryvs9sl0ht3pn3mxghuma64jst5"));

    block_on(mm_alice.stop()).unwrap();
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn qrc20_activate_electrum() {
    let passphrase = "cV463HpebE2djP9ugJry5wZ9st5cc6AbkHXGryZVPXMH1XJK8cVU";
    let coins = json! ([
        {"coin":"QRC20","required_confirmations":0,"pubtype": 120,"p2shtype": 50,"wiftype": 128,"segwit": true,"txfee": 0,"mm2": 1,"mature_confirmations":2000,
         "protocol":{"type":"QRC20","protocol_data":{"platform":"QTUM","contract_address":"0xd362e096e873eb7907e205fadc6175c6fec7bc44"}}},
    ]);

    let mm = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 8999,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": passphrase,
            "coins": coins,
            "rpc_password": "password",
            "i_am_seed": true,
        }),
        "password".into(),
        local_start!("bob"),
    )
    .unwrap();

    let (_bob_dump_log, _bob_dump_dashboard) = mm.mm_dump();
    log! ({"Bob log path: {}", mm.log_path.display()});
    let electrum_json = block_on(enable_qrc20(
        &mm,
        "QRC20",
        &["95.217.83.126:10001"],
        "0xba8b71f3544b93e2f681f996da519a98ace0107a",
    ));
    assert_eq!(
        electrum_json["address"].as_str(),
        Some("qKEDGuogDhtH9zBnc71QtqT1KDamaR1KJ3")
    );
    assert_eq!(electrum_json["balance"].as_str(), Some("139"));
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_qrc20_withdraw() {
    // corresponding private key: [3, 98, 177, 3, 108, 39, 234, 144, 131, 178, 103, 103, 127, 80, 230, 166, 53, 68, 147, 215, 42, 216, 144, 72, 172, 110, 180, 13, 123, 179, 10, 49]
    let passphrase = "cMhHM3PMpMrChygR4bLF7QsTdenhWpFrrmf2UezBG3eeFsz41rtL";
    let coins = json!([
        {"coin":"QRC20","required_confirmations":0,"pubtype": 120,"p2shtype": 50,"wiftype": 128,"segwit": true,"txfee": 0,"mm2": 1,"mature_confirmations":2000,
         "protocol":{"type":"QRC20","protocol_data":{"platform":"QTUM","contract_address":"0xd362e096e873eb7907e205fadc6175c6fec7bc44"}}},
    ]);

    let mm = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 8999,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": passphrase,
            "coins": coins,
            "rpc_password": "password",
            "i_am_seed": true,
        }),
        "password".into(),
        local_start!("bob"),
    )
    .unwrap();

    let (_bob_dump_log, _bob_dump_dashboard) = mm.mm_dump();
    log!({ "Bob log path: {}", mm.log_path.display() });

    let electrum_json = block_on(enable_qrc20(
        &mm,
        "QRC20",
        &["95.217.83.126:10001"],
        "0xba8b71f3544b93e2f681f996da519a98ace0107a",
    ));
    assert_eq!(
        electrum_json["address"].as_str(),
        Some("qXxsj5RtciAby9T7m98AgAATL4zTi4UwDG")
    );
    log!("electrum_json: "[electrum_json]);
    let balance: f64 = electrum_json["balance"].as_str().unwrap().parse().unwrap();
    log!("Balance "(balance));

    let amount = 10;

    let withdraw = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "withdraw",
        "coin": "QRC20",
        "to": "qHmJ3KA6ZAjR9wGjpFASn4gtUSeFAqdZgs",
        "amount": amount,
        "fee": {
            "type": "Qrc20Gas",
            "gas_limit": 2_500_000,
            "gas_price": 40,
        }
    })))
    .unwrap();

    let withdraw_json: Json = json::from_str(&withdraw.1).unwrap();
    assert!(withdraw.0.is_success(), "QRC20 withdraw: {}", withdraw.1);

    log!((withdraw_json));
    assert!(withdraw_json["tx_hex"].as_str().unwrap().contains("5403a02526012844a9059cbb0000000000000000000000000240b898276ad2cc0d2fe6f527e8e31104e7fde3000000000000000000000000000000000000000000000000000000003b9aca0014d362e096e873eb7907e205fadc6175c6fec7bc44c2"));

    let send_tx = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "send_raw_transaction",
        "coin": "QRC20",
        "tx_hex": withdraw_json["tx_hex"],
    })))
    .unwrap();
    assert!(send_tx.0.is_success(), "QRC20 send_raw_transaction: {}", send_tx.1);
    log!((send_tx.1));
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_qrc20_withdraw_error() {
    let passphrase = "album hollow help heart use bird response large lounge fat elbow coral";
    let coins = json!([
        {"coin":"QRC20","required_confirmations":0,"pubtype": 120,"p2shtype": 50,"wiftype": 128,"segwit": true,"txfee": 0,"mm2": 1,"mature_confirmations":2000,
         "protocol":{"type":"QRC20","protocol_data":{"platform":"QTUM","contract_address":"0xd362e096e873eb7907e205fadc6175c6fec7bc44"}}},
    ]);

    let mm = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 8999,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": passphrase,
            "coins": coins,
            "rpc_password": "password",
            "i_am_seed": true,
        }),
        "password".into(),
        local_start!("bob"),
    )
    .unwrap();
    let (_dump_log, _dump_dashboard) = mm.mm_dump();
    log!({ "log path: {}", mm.log_path.display() });

    let electrum_json = block_on(enable_qrc20(
        &mm,
        "QRC20",
        &["95.217.83.126:10001"],
        "0xba8b71f3544b93e2f681f996da519a98ace0107a",
    ));
    let balance = electrum_json["balance"].as_str().unwrap();
    assert_eq!(balance, "10");

    // try to transfer more than balance
    let withdraw = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "withdraw",
        "coin": "QRC20",
        "to": "qHmJ3KA6ZAjR9wGjpFASn4gtUSeFAqdZgs",
        "amount": "11",
    })))
    .unwrap();
    assert!(
        withdraw.0.is_server_error(),
        "withdraw should have failed, but got {:?}",
        withdraw
    );
    log!([withdraw.1]);
    assert!(withdraw
        .1
        .contains("Not enough QRC20 to withdraw: available 10, required at least 11"));

    // try to transfer with zero QTUM balance
    let withdraw = block_on(mm.rpc(json! ({
        "userpass": mm.userpass,
        "method": "withdraw",
        "coin": "QRC20",
        "to": "qHmJ3KA6ZAjR9wGjpFASn4gtUSeFAqdZgs",
        "amount": "2",
        "fee": {
            "type": "Qrc20Gas",
            "gas_limit": 100_000,
            "gas_price": 40,
        }
    })))
    .unwrap();
    assert!(
        withdraw.0.is_server_error(),
        "withdraw should have failed, but got {:?}",
        withdraw
    );
    log!([withdraw.1]);
    // 0.04 = 100_000 * 40 / 100_000_000
    assert!(withdraw
        .1
        .contains("Not enough QTUM to withdraw: available 0, required at least 0.04"));
}

async fn test_qrc20_history_impl() {
    let passphrase = "daring blind measure rebuild grab boost fix favorite nurse stereo april rookie";
    let coins = json!([
        {"coin":"QRC20","required_confirmations":0,"pubtype": 120,"p2shtype": 50,"wiftype": 128,"segwit": true,"txfee": 0,"mm2": 1,"mature_confirmations":2000,
         "protocol":{"type":"QRC20","protocol_data":{"platform":"QTUM","contract_address":"0xd362e096e873eb7907e205fadc6175c6fec7bc44"}}},
    ]);

    let mut mm = MarketMakerIt::start_async(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "passphrase": passphrase,
            "coins": coins,
            "rpc_password": "pass",
            "metrics_interval": 30.,
        }),
        "pass".into(),
        local_start!("bob"),
    )
    .await
    .unwrap();
    let (_dump_log, _dump_dashboard) = mm.mm_dump();

    #[cfg(not(target_arch = "wasm32"))]
    common::log::info!("log path: {}", mm.log_path.display());

    mm.wait_for_log(22., |log| log.contains(">>>>>>>>> DEX stats "))
        .await
        .unwrap();

    let electrum = mm
        .rpc(json!({
            "userpass": mm.userpass,
            "method": "electrum",
            "coin": "QRC20",
            "servers": qtum_electrums(),
            "mm2": 1,
            "tx_history": true,
            "swap_contract_address": "0xd362e096e873eb7907e205fadc6175c6fec7bc44",
        }))
        .await
        .unwrap();
    assert_eq!(
        electrum.0,
        StatusCode::OK,
        "RPC «electrum» failed with status «{}», response «{}»",
        electrum.0,
        electrum.1
    );
    let electrum_json: Json = json::from_str(&electrum.1).unwrap();
    assert_eq!(
        electrum_json["address"].as_str(),
        Some("qfkXE2cNFEwPFQqvBcqs8m9KrkNa9KV4xi")
    );

    // Wait till tx_history will not be loaded
    mm.wait_for_log(22., |log| log.contains("history has been loaded successfully"))
        .await
        .unwrap();

    // let the MarketMaker save the history to the file
    Timer::sleep(1.).await;

    let tx_history = mm
        .rpc(json!({
            "userpass": mm.userpass,
            "method": "my_tx_history",
            "coin": "QRC20",
            "limit": 100,
        }))
        .await
        .unwrap();
    assert_eq!(
        tx_history.0,
        StatusCode::OK,
        "RPC «my_tx_history» failed with status «{}», response «{}»",
        tx_history.0,
        tx_history.1
    );
    common::log::debug!("{:?}", tx_history.1);
    let tx_history_json: Json = json::from_str(&tx_history.1).unwrap();
    let tx_history_result = &tx_history_json["result"];

    let mut expected = vec![
        // https://testnet.qtum.info/tx/45d722e615feb853d608033ffc20fd51c9ee86e2321cfa814ba5961190fb57d2
        "45d722e615feb853d608033ffc20fd51c9ee86e2321cfa814ba5961190fb57d200000000000000020000000000000000",
        // https://testnet.qtum.info/tx/45d722e615feb853d608033ffc20fd51c9ee86e2321cfa814ba5961190fb57d2
        "45d722e615feb853d608033ffc20fd51c9ee86e2321cfa814ba5961190fb57d200000000000000020000000000000001",
        // https://testnet.qtum.info/tx/abcb51963e720fdfed7b889cea79947ba3cabd7b8b384f6b5adb41a3f4b5d61b
        "abcb51963e720fdfed7b889cea79947ba3cabd7b8b384f6b5adb41a3f4b5d61b00000000000000020000000000000000",
        // https://testnet.qtum.info/tx/4ea5392d03a9c35126d2d5a8294c3c3102cfc6d65235897c92ca04c5515f6be5
        "4ea5392d03a9c35126d2d5a8294c3c3102cfc6d65235897c92ca04c5515f6be500000000000000020000000000000000",
        // https://testnet.qtum.info/tx/9156f5f1d3652c27dca0216c63177da38de5c9e9f03a5cfa278bf82882d2d3d8
        "9156f5f1d3652c27dca0216c63177da38de5c9e9f03a5cfa278bf82882d2d3d800000000000000020000000000000000",
        // https://testnet.qtum.info/tx/35e03bc529528a853ee75dde28f27eec8ed7b152b6af7ab6dfa5d55ea46f25ac
        "35e03bc529528a853ee75dde28f27eec8ed7b152b6af7ab6dfa5d55ea46f25ac00000000000000010000000000000000",
        // https://testnet.qtum.info/tx/39104d29d77ba83c5c6c63ab7a0f096301c443b4538dc6b30140453a40caa80a
        "39104d29d77ba83c5c6c63ab7a0f096301c443b4538dc6b30140453a40caa80a00000000000000000000000000000000",
        // https://testnet.qtum.info/tx/d9965e3496a8a4af2d462424b989694b3146d78c61654b99bbadba64464f75cb
        "d9965e3496a8a4af2d462424b989694b3146d78c61654b99bbadba64464f75cb00000000000000000000000000000000",
        // https://testnet.qtum.info/tx/c2f346d3d2aadc35f5343d0d493a139b2579175496d685ec30734d161e62f7a1
        "c2f346d3d2aadc35f5343d0d493a139b2579175496d685ec30734d161e62f7a100000000000000000000000000000000",
    ];

    assert_eq!(tx_history_result["total"].as_u64().unwrap(), expected.len() as u64);
    for tx in tx_history_result["transactions"].as_array().unwrap() {
        // pop front item
        let expected_tx = expected.remove(0);
        assert_eq!(tx["internal_id"].as_str().unwrap(), expected_tx);
    }
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_qrc20_tx_history() { block_on(test_qrc20_history_impl()); }

#[wasm_bindgen_test]
#[cfg(target_arch = "wasm32")]
async fn test_qrc20_tx_history() { test_qrc20_history_impl().await }

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_buy_conf_settings() {
    let bob_passphrase = get_passphrase(&".env.client", "BOB_PASSPHRASE").unwrap();

    let coins = json! ([
        {"coin":"RICK","asset":"RICK","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"}},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}},"required_confirmations":2}
    ]);

    let mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 8999,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": bob_passphrase,
            "coins": coins,
            "rpc_password": "password",
            "i_am_seed": true,
        }),
        "password".into(),
        local_start!("bob"),
    )
    .unwrap();

    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log! ({"Bob log path: {}", mm_bob.log_path.display()});
    log!([block_on(enable_coins_eth_electrum(&mm_bob, &[
        "http://195.201.0.6:8565"
    ]))]);

    log!("Issue bob buy request");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "buy",
        "base": "ETH",
        "rel": "JST",
        "price": 1,
        "volume": 0.1,
        "base_confs": 5,
        "base_nota": true,
        "rel_confs": 4,
        "rel_nota": false,
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!buy: {}", rc.1);
    let json: Json = json::from_str(&rc.1).unwrap();
    assert_eq!(json["result"]["conf_settings"]["base_confs"], Json::from(5));
    assert_eq!(json["result"]["conf_settings"]["base_nota"], Json::from(true));
    assert_eq!(json["result"]["conf_settings"]["rel_confs"], Json::from(4));
    assert_eq!(json["result"]["conf_settings"]["rel_nota"], Json::from(false));

    // must use coin config as defaults if not set in request
    log!("Issue bob buy request");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "buy",
        "base": "ETH",
        "rel": "JST",
        "price": 1,
        "volume": 0.1,
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!buy: {}", rc.1);
    let json: Json = json::from_str(&rc.1).unwrap();
    assert_eq!(json["result"]["conf_settings"]["base_confs"], Json::from(1));
    assert_eq!(json["result"]["conf_settings"]["base_nota"], Json::from(false));
    assert_eq!(json["result"]["conf_settings"]["rel_confs"], Json::from(2));
    assert_eq!(json["result"]["conf_settings"]["rel_nota"], Json::from(false));
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_buy_response_format() {
    let bob_passphrase = get_passphrase(&".env.client", "BOB_PASSPHRASE").unwrap();

    let coins = json! ([
        {"coin":"RICK","asset":"RICK","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"}},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}},"required_confirmations":2}
    ]);

    let mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 8999,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": bob_passphrase,
            "coins": coins,
            "rpc_password": "password",
            "i_am_seed": true,
        }),
        "password".into(),
        local_start!("bob"),
    )
    .unwrap();

    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log! ({"Bob log path: {}", mm_bob.log_path.display()});
    log!([block_on(enable_coins_eth_electrum(&mm_bob, &[
        "http://195.201.0.6:8565"
    ]))]);

    log!("Issue bob buy request");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "buy",
        "base": "ETH",
        "rel": "JST",
        "price": 1,
        "volume": 0.1,
        "base_confs": 5,
        "base_nota": true,
        "rel_confs": 4,
        "rel_nota": false,
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!buy: {}", rc.1);
    let _: BuyOrSellRpcResult = json::from_str(&rc.1).unwrap();
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_sell_response_format() {
    let bob_passphrase = get_passphrase(&".env.client", "BOB_PASSPHRASE").unwrap();

    let coins = json! ([
        {"coin":"RICK","asset":"RICK","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"}},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}},"required_confirmations":2}
    ]);

    let mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 8999,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": bob_passphrase,
            "coins": coins,
            "rpc_password": "password",
            "i_am_seed": true,
        }),
        "password".into(),
        local_start!("bob"),
    )
    .unwrap();

    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log! ({"Bob log path: {}", mm_bob.log_path.display()});
    log!([block_on(enable_coins_eth_electrum(&mm_bob, &[
        "http://195.201.0.6:8565"
    ]))]);

    log!("Issue bob sell request");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "sell",
        "base": "ETH",
        "rel": "JST",
        "price": 1,
        "volume": 0.1,
        "base_confs": 5,
        "base_nota": true,
        "rel_confs": 4,
        "rel_nota": false,
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!sell: {}", rc.1);
    let _: BuyOrSellRpcResult = json::from_str(&rc.1).unwrap();
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_my_orders_response_format() {
    let bob_passphrase = get_passphrase(&".env.client", "BOB_PASSPHRASE").unwrap();

    let coins = json! ([
        {"coin":"RICK","asset":"RICK","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"}},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}},"required_confirmations":2}
    ]);

    let mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 8999,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": bob_passphrase,
            "coins": coins,
            "rpc_password": "password",
            "i_am_seed": true,
        }),
        "password".into(),
        local_start!("bob"),
    )
    .unwrap();

    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log! ({"Bob log path: {}", mm_bob.log_path.display()});
    log!([block_on(enable_coins_eth_electrum(&mm_bob, &[
        "http://195.201.0.6:8565"
    ]))]);

    log!("Issue bob buy request");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "buy",
        "base": "ETH",
        "rel": "JST",
        "price": 1,
        "volume": 0.1,
        "base_confs": 5,
        "base_nota": true,
        "rel_confs": 4,
        "rel_nota": false,
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!buy: {}", rc.1);

    log!("Issue bob setprice request");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "setprice",
        "base": "ETH",
        "rel": "JST",
        "price": 1,
        "volume": 0.1,
        "base_confs": 5,
        "base_nota": true,
        "rel_confs": 4,
        "rel_nota": false,
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!setprice: {}", rc.1);

    log!("Issue bob my_orders request");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "my_orders",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!my_orders: {}", rc.1);

    let _: MyOrdersRpcResult = json::from_str(&rc.1).unwrap();
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_my_orders_after_matched() {
    let bob_passphrase = get_passphrase(&".env.seed", "BOB_PASSPHRASE").unwrap();
    let alice_passphrase = get_passphrase(&".env.client", "ALICE_PASSPHRASE").unwrap();

    let coins = json! ([
        {"coin":"RICK","asset":"RICK","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"}},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}}}
    ]);

    let mut mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9000,
            "dht": "on",  // Enable DHT without delay.
            "passphrase": bob_passphrase,
            "coins": coins,
            "rpc_password": "pass",
            "i_am_seed": true,
        }),
        "pass".to_string(),
        None,
    )
    .unwrap();
    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();

    let mut mm_alice = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9000,
            "dht": "on",  // Enable DHT without delay.
            "passphrase": alice_passphrase,
            "coins": coins,
            "rpc_password": "pass",
            "seednodes": vec![format!("{}", mm_bob.ip)],
        }),
        "pass".to_string(),
        None,
    )
    .unwrap();
    let (_alice_dump_log, _alice_dump_dashboard) = mm_alice.mm_dump();

    // Enable coins on Bob side. Print the replies in case we need the address.
    let rc = block_on(enable_coins_eth_electrum(&mm_bob, &["http://195.201.0.6:8565"]));
    log! ({"enable_coins (bob): {:?}", rc});
    // Enable coins on Alice side. Print the replies in case we need the address.
    let rc = block_on(enable_coins_eth_electrum(&mm_alice, &["http://195.201.0.6:8565"]));
    log! ({"enable_coins (alice): {:?}", rc});

    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "setprice",
        "base": "ETH",
        "rel": "JST",
        "price": 1,
        "volume": 2,
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!setprice: {}", rc.1);

    let rc = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "method": "buy",
        "base": "ETH",
        "rel": "JST",
        "price": 1,
        "volume": 1,
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!buy: {}", rc.1);

    block_on(mm_bob.wait_for_log(22., |log| log.contains("Entering the maker_swap_loop ETH/JST"))).unwrap();
    block_on(mm_alice.wait_for_log(22., |log| log.contains("Entering the taker_swap_loop ETH/JST"))).unwrap();

    log!("Issue bob my_orders request");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "my_orders",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!my_orders: {}", rc.1);

    let _: MyOrdersRpcResult = json::from_str(&rc.1).unwrap();
    block_on(mm_bob.stop()).unwrap();
    block_on(mm_alice.stop()).unwrap();
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_sell_conf_settings() {
    let bob_passphrase = get_passphrase(&".env.client", "BOB_PASSPHRASE").unwrap();

    let coins = json! ([
        {"coin":"RICK","asset":"RICK","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"}},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}},"required_confirmations":2}
    ]);

    let mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 8999,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": bob_passphrase,
            "coins": coins,
            "rpc_password": "password",
            "i_am_seed": true,
        }),
        "password".into(),
        local_start!("bob"),
    )
    .unwrap();

    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log! ({"Bob log path: {}", mm_bob.log_path.display()});
    log!([block_on(enable_coins_eth_electrum(&mm_bob, &[
        "http://195.201.0.6:8565"
    ]))]);

    log!("Issue bob sell request");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "sell",
        "base": "ETH",
        "rel": "JST",
        "price": 1,
        "volume": 0.1,
        "base_confs": 5,
        "base_nota": true,
        "rel_confs": 4,
        "rel_nota": false,
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!sell: {}", rc.1);
    let json: Json = json::from_str(&rc.1).unwrap();
    assert_eq!(json["result"]["conf_settings"]["base_confs"], Json::from(5));
    assert_eq!(json["result"]["conf_settings"]["base_nota"], Json::from(true));
    assert_eq!(json["result"]["conf_settings"]["rel_confs"], Json::from(4));
    assert_eq!(json["result"]["conf_settings"]["rel_nota"], Json::from(false));

    // must use coin config as defaults if not set in request
    log!("Issue bob sell request");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "sell",
        "base": "ETH",
        "rel": "JST",
        "price": 1,
        "volume": 0.1,
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!sell: {}", rc.1);
    let json: Json = json::from_str(&rc.1).unwrap();
    assert_eq!(json["result"]["conf_settings"]["base_confs"], Json::from(1));
    assert_eq!(json["result"]["conf_settings"]["base_nota"], Json::from(false));
    assert_eq!(json["result"]["conf_settings"]["rel_confs"], Json::from(2));
    assert_eq!(json["result"]["conf_settings"]["rel_nota"], Json::from(false));
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_set_price_conf_settings() {
    let bob_passphrase = get_passphrase(&".env.client", "BOB_PASSPHRASE").unwrap();

    let coins = json! ([
        {"coin":"RICK","asset":"RICK","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"}},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}},"required_confirmations":2}
    ]);

    let mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 8999,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": bob_passphrase,
            "coins": coins,
            "rpc_password": "password",
            "i_am_seed": true,
        }),
        "password".into(),
        local_start!("bob"),
    )
    .unwrap();

    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log! ({"Bob log path: {}", mm_bob.log_path.display()});
    log!([block_on(enable_coins_eth_electrum(&mm_bob, &[
        "http://195.201.0.6:8565"
    ]))]);

    log!("Issue bob sell request");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "setprice",
        "base": "ETH",
        "rel": "JST",
        "price": 1,
        "volume": 0.1,
        "base_confs": 5,
        "base_nota": true,
        "rel_confs": 4,
        "rel_nota": false,
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!setprice: {}", rc.1);
    let json: Json = json::from_str(&rc.1).unwrap();
    assert_eq!(json["result"]["conf_settings"]["base_confs"], Json::from(5));
    assert_eq!(json["result"]["conf_settings"]["base_nota"], Json::from(true));
    assert_eq!(json["result"]["conf_settings"]["rel_confs"], Json::from(4));
    assert_eq!(json["result"]["conf_settings"]["rel_nota"], Json::from(false));

    // must use coin config as defaults if not set in request
    log!("Issue bob sell request");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "setprice",
        "base": "ETH",
        "rel": "JST",
        "price": 1,
        "volume": 0.1,
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!setprice: {}", rc.1);
    let json: Json = json::from_str(&rc.1).unwrap();
    assert_eq!(json["result"]["conf_settings"]["base_confs"], Json::from(1));
    assert_eq!(json["result"]["conf_settings"]["base_nota"], Json::from(false));
    assert_eq!(json["result"]["conf_settings"]["rel_confs"], Json::from(2));
    assert_eq!(json["result"]["conf_settings"]["rel_nota"], Json::from(false));
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_update_maker_order() {
    let bob_passphrase = get_passphrase(&".env.client", "BOB_PASSPHRASE").unwrap();

    let coins = json! ([
        {"coin":"RICK","asset":"RICK","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"}},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}},"required_confirmations":2}
    ]);

    let mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 8999,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": bob_passphrase,
            "coins": coins,
            "rpc_password": "password",
            "i_am_seed": true,
        }),
        "password".into(),
        local_start!("bob"),
    )
    .unwrap();

    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log! ({"Bob log path: {}", mm_bob.log_path.display()});
    log!([block_on(enable_coins_eth_electrum(&mm_bob, &[
        "http://195.201.0.6:8565"
    ]))]);

    log!("Get bob balance");
    let my_balance = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "my_balance",
        "coin": "RICK",
    })))
    .unwrap();
    assert!(my_balance.0.is_success(), "!my_balance: {}", my_balance.1);
    let my_balance_json: Json = json::from_str(&my_balance.1).unwrap();
    let balance: BigDecimal = json::from_value(my_balance_json["balance"].clone()).unwrap();

    log!("Get RICK trade fee");
    let get_trade_fee = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "get_trade_fee",
        "coin": "RICK",
    })))
    .unwrap();
    assert!(get_trade_fee.0.is_success(), "!get_trade_fee: {}", get_trade_fee.1);
    let get_trade_fee_json: Json = json::from_str(&get_trade_fee.1).unwrap();
    let trade_fee: BigDecimal = json::from_value(get_trade_fee_json["result"]["amount"].clone()).unwrap();
    let max_volume = balance - trade_fee;

    log!("Issue bob sell request");
    let setprice = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "setprice",
        "base": "RICK",
        "rel": "MORTY",
        "price": 1,
        "volume": 2,
        "min_volume": 1,
        "base_confs": 5,
        "base_nota": true,
        "rel_confs": 4,
        "rel_nota": false,
    })))
    .unwrap();
    assert!(setprice.0.is_success(), "!setprice: {}", setprice.1);
    let setprice_json: Json = json::from_str(&setprice.1).unwrap();
    let uuid: Uuid = json::from_value(setprice_json["result"]["uuid"].clone()).unwrap();

    log!("Issue bob update maker order request");
    let update_maker_order = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "update_maker_order",
        "uuid": uuid,
        "new_price": 2,
    })))
    .unwrap();
    assert!(
        update_maker_order.0.is_success(),
        "!update_maker_order: {}",
        update_maker_order.1
    );
    let update_maker_order_json: Json = json::from_str(&update_maker_order.1).unwrap();
    assert_eq!(update_maker_order_json["result"]["price"], Json::from("2"));
    assert_eq!(update_maker_order_json["result"]["max_base_vol"], Json::from("2"));
    assert_eq!(update_maker_order_json["result"]["min_base_vol"], Json::from("1"));

    log!("Issue another bob update maker order request");
    let update_maker_order = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "update_maker_order",
        "uuid": uuid,
        "volume_delta": 2,
    })))
    .unwrap();
    assert!(
        update_maker_order.0.is_success(),
        "!update_maker_order: {}",
        update_maker_order.1
    );
    let update_maker_order_json: Json = json::from_str(&update_maker_order.1).unwrap();
    assert_eq!(update_maker_order_json["result"]["price"], Json::from("2"));
    assert_eq!(update_maker_order_json["result"]["max_base_vol"], Json::from("4"));
    assert_eq!(update_maker_order_json["result"]["min_base_vol"], Json::from("1"));

    log!("Issue another bob update maker order request");
    let update_maker_order = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "update_maker_order",
        "uuid": uuid,
        "max": true,
    })))
    .unwrap();
    assert!(
        update_maker_order.0.is_success(),
        "!update_maker_order: {}",
        update_maker_order.1
    );
    let update_maker_order_json: Json = json::from_str(&update_maker_order.1).unwrap();
    let max_base_vol =
        BigDecimal::from_str(update_maker_order_json["result"]["max_base_vol"].as_str().unwrap()).unwrap();
    assert_eq!(update_maker_order_json["result"]["price"], Json::from("2"));
    assert_eq!(max_base_vol, max_volume);
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_update_maker_order_fail() {
    let bob_passphrase = get_passphrase(&".env.client", "BOB_PASSPHRASE").unwrap();

    let coins = json! ([
        {"coin":"RICK","asset":"RICK","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"}},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}},"required_confirmations":2}
    ]);

    let mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 8999,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": bob_passphrase,
            "coins": coins,
            "rpc_password": "password",
            "i_am_seed": true,
        }),
        "password".into(),
        local_start!("bob"),
    )
    .unwrap();

    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log! ({"Bob log path: {}", mm_bob.log_path.display()});
    log!([block_on(enable_coins_eth_electrum(&mm_bob, &[
        "http://195.201.0.6:8565"
    ]))]);

    log!("Issue bob sell request");
    let setprice = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "setprice",
        "base": "RICK",
        "rel": "MORTY",
        "price": 1,
        "volume": 0.1,
        "base_confs": 5,
        "base_nota": true,
        "rel_confs": 4,
        "rel_nota": false,
    })))
    .unwrap();
    assert!(setprice.0.is_success(), "!setprice: {}", setprice.1);
    let setprice_json: Json = json::from_str(&setprice.1).unwrap();
    let uuid: Uuid = json::from_value(setprice_json["result"]["uuid"].clone()).unwrap();

    log!("Issue bob update maker order request that should fail because price is too low");
    let update_maker_order = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "update_maker_order",
        "uuid": uuid,
        "new_price": 0.0000000099,
    })))
    .unwrap();
    assert!(
        !update_maker_order.0.is_success(),
        "update_maker_order success, but should be error {}",
        update_maker_order.1
    );

    log!("Issue bob update maker order request that should fail because New Volume is Less than Zero");
    let update_maker_order = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "update_maker_order",
        "uuid": uuid,
        "volume_delta": -0.11,
    })))
    .unwrap();
    assert!(
        !update_maker_order.0.is_success(),
        "update_maker_order success, but should be error {}",
        update_maker_order.1
    );

    log!("Issue bob update maker order request that should fail because Min base vol is too low");
    let update_maker_order = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "update_maker_order",
        "uuid": uuid,
        "new_price": 2,
        "min_volume": 0.000099,
    })))
    .unwrap();
    assert!(
        !update_maker_order.0.is_success(),
        "update_maker_order success, but should be error {}",
        update_maker_order.1
    );

    log!("Issue bob update maker order request that should fail because Max base vol is below Min base vol");
    let update_maker_order = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "update_maker_order",
        "uuid": uuid,
        "volume_delta": -0.0999,
        "min_volume": 0.0002,
    })))
    .unwrap();
    assert!(
        !update_maker_order.0.is_success(),
        "update_maker_order success, but should be error {}",
        update_maker_order.1
    );

    log!("Issue bob update maker order request that should fail because Max base vol is too low");
    let update_maker_order = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "update_maker_order",
        "uuid": uuid,
        "new_price": 2,
        "volume_delta": -0.099901,
    })))
    .unwrap();
    assert!(
        !update_maker_order.0.is_success(),
        "update_maker_order success, but should be error {}",
        update_maker_order.1
    );

    log!("Issue bob update maker order request that should fail because Max rel vol is too low");
    let update_maker_order = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "update_maker_order",
        "uuid": uuid,
        "new_price": 0.5,
        "volume_delta": -0.099802,
    })))
    .unwrap();
    assert!(
        !update_maker_order.0.is_success(),
        "update_maker_order success, but should be error {}",
        update_maker_order.1
    );

    log!("Issue bob batch update maker order and cancel order request that should make update maker order fail because Order with UUID has been deleted");
    let batch_json = json!([
        {
            "userpass": mm_bob.userpass,
            "method": "update_maker_order",
            "uuid": uuid,
            "new_price": 1,
            "volume_delta": 2.9,
        },
        {
            "userpass": mm_bob.userpass,
            "method": "cancel_order",
            "uuid": uuid,
        },
    ]);

    let rc = block_on(mm_bob.rpc(batch_json)).unwrap();
    assert!(rc.0.is_success(), "!batch: {}", rc.1);
    log!((rc.1));
    let responses = json::from_str::<Vec<Json>>(&rc.1).unwrap();
    if responses[1]["result"] == Json::from("success") {
        assert_eq!(responses[0].get("result"), None);
    }
}
#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_update_maker_order_after_matched() {
    let bob_passphrase = get_passphrase(&".env.seed", "BOB_PASSPHRASE").unwrap();
    let alice_passphrase = get_passphrase(&".env.client", "ALICE_PASSPHRASE").unwrap();

    let coins = json! ([
        {"coin":"RICK","asset":"RICK","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"}},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}}}
    ]);

    let mut mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9000,
            "dht": "on",  // Enable DHT without delay.
            "passphrase": bob_passphrase,
            "coins": coins,
            "rpc_password": "pass",
            "i_am_seed": true,
        }),
        "pass".to_string(),
        None,
    )
    .unwrap();
    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log!({"Bob log path: {}", mm_bob.log_path.display()});

    let mut mm_alice = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9000,
            "dht": "on",  // Enable DHT without delay.
            "passphrase": alice_passphrase,
            "coins": coins,
            "rpc_password": "pass",
            "seednodes": vec![format!("{}", mm_bob.ip)],
        }),
        "pass".to_string(),
        None,
    )
    .unwrap();
    let (_alice_dump_log, _alice_dump_dashboard) = mm_alice.mm_dump();

    // Enable coins on Bob side. Print the replies in case we need the address.
    let rc = block_on(enable_coins_eth_electrum(&mm_bob, &["http://195.201.0.6:8565"]));
    log! ({"enable_coins (bob): {:?}", rc});
    // Enable coins on Alice side. Print the replies in case we need the address.
    let rc = block_on(enable_coins_eth_electrum(&mm_alice, &["http://195.201.0.6:8565"]));
    log! ({"enable_coins (alice): {:?}", rc});

    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "setprice",
        "base": "ETH",
        "rel": "JST",
        "price": 1,
        "volume": 2,
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!setprice: {}", rc.1);
    let setprice_json: Json = json::from_str(&rc.1).unwrap();
    let uuid: Uuid = json::from_value(setprice_json["result"]["uuid"].clone()).unwrap();

    let rc = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "method": "buy",
        "base": "ETH",
        "rel": "JST",
        "price": 1,
        "volume": 1,
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!buy: {}", rc.1);

    block_on(mm_bob.wait_for_log(22., |log| log.contains("Entering the maker_swap_loop ETH/JST"))).unwrap();
    block_on(mm_alice.wait_for_log(22., |log| log.contains("Entering the taker_swap_loop ETH/JST"))).unwrap();

    log!("Issue bob update maker order request that should fail because new volume is less than reserved amount");
    let update_maker_order = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "update_maker_order",
        "uuid": uuid,
        "volume_delta": -1.5,
    })))
    .unwrap();
    assert!(
        !update_maker_order.0.is_success(),
        "update_maker_order success, but should be error {}",
        update_maker_order.1
    );

    log!("Issue another bob update maker order request");
    let update_maker_order = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "update_maker_order",
        "uuid": uuid,
        "volume_delta": 2,
    })))
    .unwrap();
    assert!(
        update_maker_order.0.is_success(),
        "!update_maker_order: {}",
        update_maker_order.1
    );
    let update_maker_order_json: Json = json::from_str(&update_maker_order.1).unwrap();
    log!((update_maker_order.1));
    assert_eq!(update_maker_order_json["result"]["max_base_vol"], Json::from("4"));

    log!("Issue bob my_orders request");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "my_orders",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!my_orders: {}", rc.1);

    let _: MyOrdersRpcResult = json::from_str(&rc.1).unwrap();
    block_on(mm_bob.stop()).unwrap();
    block_on(mm_alice.stop()).unwrap();
}

// https://github.com/KomodoPlatform/atomicDEX-API/issues/683
// trade fee should return numbers in all 3 available formats and
// "amount" must be always in decimal representation for backwards compatibility
#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_trade_fee_returns_numbers_in_various_formats() {
    let coins = json!([
        {"coin":"RICK","asset":"RICK","rpcport":8923,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","rpcport":11608,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"},"rpcport":80},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0xc0eb7AeD740E1796992A08962c15661bDEB58003"}}}
    ]);

    // start bob and immediately place the order
    let mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": "bob passphrase",
            "coins": coins,
            "i_am_seed": true,
            "rpc_password": "pass",
        }),
        "pass".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "bob" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();
    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log!({"Bob log path: {}", mm_bob.log_path.display()});
    block_on(enable_coins_eth_electrum(&mm_bob, &[
        "https://ropsten.infura.io/v3/c01c1b4cf66642528547624e1d6d9d6b",
    ]));

    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "get_trade_fee",
        "coin": "RICK",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!get_trade_fee: {}", rc.1);
    let trade_fee_json: Json = json::from_str(&rc.1).unwrap();
    let _amount_dec: BigDecimal = json::from_value(trade_fee_json["result"]["amount"].clone()).unwrap();
    let _amount_rat: BigRational = json::from_value(trade_fee_json["result"]["amount_rat"].clone()).unwrap();
    let _amount_fraction: Fraction = json::from_value(trade_fee_json["result"]["amount_fraction"].clone()).unwrap();
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_orderbook_is_mine_orders() {
    let coins = json!([{"coin":"RICK","asset":"RICK","rpcport":8923,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","rpcport":11608,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"},"rpcport":80},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0xc0eb7AeD740E1796992A08962c15661bDEB58003"}}}
    ]);

    // start bob and immediately place the order
    let mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": "bob passphrase",
            "coins": coins,
            "i_am_seed": true,
            "rpc_password": "pass",
        }),
        "pass".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "bob" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();
    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log!({"Bob log path: {}", mm_bob.log_path.display()});
    // Enable coins on Bob side. Print the replies in case we need the "address".
    log! ({"enable_coins (bob): {:?}", block_on (enable_coins_eth_electrum (&mm_bob, &["https://ropsten.infura.io/v3/c01c1b4cf66642528547624e1d6d9d6b"]))});

    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "setprice",
        "base": "RICK",
        "rel": "MORTY",
        "price": 0.9,
        "volume": "0.9",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!setprice: {}", rc.1);
    let _bob_setprice: Json = json::from_str(&rc.1).unwrap();

    let mm_alice = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("ALICE_TRADE_IP") .ok(),
            "rpcip": env::var ("ALICE_TRADE_IP") .ok(),
            "passphrase": "alice passphrase",
            "coins": coins,
            "seednodes": [fomat!((mm_bob.ip))],
            "rpc_password": "pass",
        }),
        "pass".into(),
        match var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "alice" => Some(local_start()),
            _ => None,
        },
    )
    .unwrap();

    let (_alice_dump_log, _alice_dump_dashboard) = mm_alice.mm_dump();
    log!({"Alice log path: {}", mm_alice.log_path.display()});

    // Enable coins on Alice side. Print the replies in case we need the "address".
    log! ({"enable_coins (alice): {:?}", block_on (enable_coins_eth_electrum (&mm_alice, &["https://ropsten.infura.io/v3/c01c1b4cf66642528547624e1d6d9d6b"]))});

    // Bob orderbook must show 1 mine order
    log!("Get RICK/MORTY orderbook on Bob side");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "orderbook",
        "base": "RICK",
        "rel": "MORTY",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!orderbook: {}", rc.1);

    let bob_orderbook: Json = json::from_str(&rc.1).unwrap();
    log!("Bob orderbook "[bob_orderbook]);
    let asks = bob_orderbook["asks"].as_array().unwrap();
    assert_eq!(asks.len(), 1, "Bob RICK/MORTY orderbook must have exactly 1 ask");
    let is_mine = asks[0]["is_mine"].as_bool().unwrap();
    assert_eq!(is_mine, true);

    // Alice orderbook must show 1 not-mine order
    log!("Get RICK/MORTY orderbook on Alice side");
    let rc = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "method": "orderbook",
        "base": "RICK",
        "rel": "MORTY",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!orderbook: {}", rc.1);

    let alice_orderbook: Json = json::from_str(&rc.1).unwrap();
    log!("Alice orderbook "[alice_orderbook]);
    let asks = alice_orderbook["asks"].as_array().unwrap();
    assert_eq!(asks.len(), 1, "Alice RICK/MORTY orderbook must have exactly 1 ask");
    let is_mine = asks[0]["is_mine"].as_bool().unwrap();
    assert_eq!(is_mine, false);

    // make another order by Alice
    let rc = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "method": "setprice",
        "base": "RICK",
        "rel": "MORTY",
        "price": 1,
        "volume": 0.1,
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!buy: {}", rc.1);

    log!("Give Bob 2 seconds to import the order…");
    thread::sleep(Duration::from_secs(2));

    // Bob orderbook must show 1 mine and 1 non-mine orders.
    // Request orderbook with reverse base and rel coins to check bids instead of asks
    log!("Get RICK/MORTY orderbook on Bob side");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "orderbook",
        "base": "MORTY",
        "rel": "RICK",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!orderbook: {}", rc.1);

    let bob_orderbook: Json = json::from_str(&rc.1).unwrap();
    log!("Bob orderbook "[bob_orderbook]);
    let asks = bob_orderbook["asks"].as_array().unwrap();
    let bids = bob_orderbook["bids"].as_array().unwrap();
    assert!(asks.is_empty(), "Bob MORTY/RICK orderbook must contain an empty asks");
    assert_eq!(bids.len(), 2, "Bob MORTY/RICK orderbook must have exactly 2 bids");
    let mine_orders = bids.iter().filter(|bid| bid["is_mine"].as_bool().unwrap()).count();
    assert_eq!(mine_orders, 1, "Bob RICK/MORTY orderbook must have exactly 1 mine bid");

    // Alice orderbook must show 1 mine and 1 non-mine orders
    log!("Get RICK/MORTY orderbook on Alice side");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "orderbook",
        "base": "RICK",
        "rel": "MORTY",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!orderbook: {}", rc.1);

    let alice_orderbook: Json = json::from_str(&rc.1).unwrap();
    log!("Alice orderbook "[alice_orderbook]);
    let asks = alice_orderbook["asks"].as_array().unwrap();
    let bids = alice_orderbook["bids"].as_array().unwrap();
    assert!(bids.is_empty(), "Alice MORTY/RICK orderbook must contain an empty bids");
    assert_eq!(asks.len(), 2, "Alice MORTY/RICK orderbook must have exactly 2 asks");
    let mine_orders = asks.iter().filter(|ask| ask["is_mine"].as_bool().unwrap()).count();
    assert_eq!(
        mine_orders, 1,
        "Alice RICK/MORTY orderbook must have exactly 1 mine bid"
    );
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_sell_min_volume() {
    let bob_passphrase = get_passphrase(&".env.client", "BOB_PASSPHRASE").unwrap();

    let coins = json! ([
        {"coin":"RICK","asset":"RICK","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"}},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}}}
    ]);

    let mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 8999,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": bob_passphrase,
            "coins": coins,
            "rpc_password": "password",
            "i_am_seed": true,
        }),
        "password".into(),
        local_start!("bob"),
    )
    .unwrap();

    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log! ({"Bob log path: {}", mm_bob.log_path.display()});
    log!([block_on(enable_coins_eth_electrum(&mm_bob, &[
        "http://195.201.0.6:8565"
    ]))]);

    let min_volume: BigDecimal = "0.1".parse().unwrap();
    log!("Issue bob ETH/JST sell request");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "sell",
        "base": "ETH",
        "rel": "JST",
        "price": "1",
        "volume": "1",
        "min_volume": min_volume,
        "order_type": {
            "type": "GoodTillCancelled"
        },
        "timeout": 2,
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!sell: {}", rc.1);
    let rc_json: Json = json::from_str(&rc.1).unwrap();
    let uuid: Uuid = json::from_value(rc_json["result"]["uuid"].clone()).unwrap();
    let min_volume_response: BigDecimal = json::from_value(rc_json["result"]["min_volume"].clone()).unwrap();
    assert_eq!(min_volume, min_volume_response);

    log!("Wait for 4 seconds for Bob order to be converted to maker");
    thread::sleep(Duration::from_secs(4));

    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "my_orders",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!my_orders: {}", rc.1);
    let my_orders: Json = json::from_str(&rc.1).unwrap();
    let my_maker_orders: HashMap<Uuid, Json> = json::from_value(my_orders["result"]["maker_orders"].clone()).unwrap();
    let my_taker_orders: HashMap<Uuid, Json> = json::from_value(my_orders["result"]["taker_orders"].clone()).unwrap();
    assert_eq!(1, my_maker_orders.len(), "maker_orders must have exactly 1 order");
    assert!(my_taker_orders.is_empty(), "taker_orders must be empty");
    let maker_order = my_maker_orders.get(&uuid).unwrap();
    let min_volume_maker: BigDecimal = json::from_value(maker_order["min_base_vol"].clone()).unwrap();
    assert_eq!(min_volume, min_volume_maker);
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_sell_min_volume_dust() {
    let bob_passphrase = get_passphrase(&".env.client", "BOB_PASSPHRASE").unwrap();

    let coins = json! ([
        {"coin":"RICK","asset":"RICK","dust":10000000,"required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"}},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}}}
    ]);

    let mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 8999,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": bob_passphrase,
            "coins": coins,
            "rpc_password": "password",
            "i_am_seed": true,
        }),
        "password".into(),
        local_start!("bob"),
    )
    .unwrap();

    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log! ({"Bob log path: {}", mm_bob.log_path.display()});
    log!([block_on(enable_coins_eth_electrum(&mm_bob, &[
        "http://195.201.0.6:8565"
    ]))]);

    log!("Issue bob RICK/MORTY sell request");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "sell",
        "base": "RICK",
        "rel": "MORTY",
        "price": "1",
        "volume": "1",
        "order_type": {
            "type": "FillOrKill"
        }
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!sell: {}", rc.1);
    let response: BuyOrSellRpcResult = json::from_str(&rc.1).unwrap();
    let expected_min = BigDecimal::from(1);
    assert_eq!(response.result.min_volume, expected_min);
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_setprice_min_volume_dust() {
    let bob_passphrase = get_passphrase(&".env.client", "BOB_PASSPHRASE").unwrap();

    let coins = json! ([
        {"coin":"RICK","asset":"RICK","dust":10000000,"required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"}},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}}}
    ]);

    let mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 8999,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": bob_passphrase,
            "coins": coins,
            "rpc_password": "password",
            "i_am_seed": true,
        }),
        "password".into(),
        local_start!("bob"),
    )
    .unwrap();

    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log! ({"Bob log path: {}", mm_bob.log_path.display()});
    log!([block_on(enable_coins_eth_electrum(&mm_bob, &[
        "http://195.201.0.6:8565"
    ]))]);

    log!("Issue bob RICK/MORTY sell request");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "setprice",
        "base": "RICK",
        "rel": "MORTY",
        "price": "1",
        "volume": "1",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!setprice: {}", rc.1);
    let response: SetPriceResponse = json::from_str(&rc.1).unwrap();
    let expected_min = BigDecimal::from(1);
    assert_eq!(expected_min, response.result.min_base_vol);
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_buy_min_volume() {
    let bob_passphrase = get_passphrase(&".env.client", "BOB_PASSPHRASE").unwrap();

    let coins = json! ([
        {"coin":"RICK","asset":"RICK","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"}},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}}}
    ]);

    let mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 8999,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": bob_passphrase,
            "coins": coins,
            "rpc_password": "password",
            "i_am_seed": true,
        }),
        "password".into(),
        local_start!("bob"),
    )
    .unwrap();

    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log! ({"Bob log path: {}", mm_bob.log_path.display()});
    log!([block_on(enable_coins_eth_electrum(&mm_bob, &[
        "http://195.201.0.6:8565"
    ]))]);

    let min_volume: BigDecimal = "0.1".parse().unwrap();
    log!("Issue bob ETH/JST sell request");
    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "buy",
        "base": "ETH",
        "rel": "JST",
        "price": "2",
        "volume": "1",
        "min_volume": min_volume,
        "order_type": {
            "type": "GoodTillCancelled"
        },
        "timeout": 2,
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!sell: {}", rc.1);
    let response: BuyOrSellRpcResult = json::from_str(&rc.1).unwrap();
    assert_eq!(min_volume, response.result.min_volume);

    log!("Wait for 4 seconds for Bob order to be converted to maker");
    thread::sleep(Duration::from_secs(4));

    let rc = block_on(mm_bob.rpc(json! ({
        "userpass": mm_bob.userpass,
        "method": "my_orders",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!my_orders: {}", rc.1);
    let my_orders: MyOrdersRpcResult = json::from_str(&rc.1).unwrap();
    assert_eq!(
        1,
        my_orders.result.maker_orders.len(),
        "maker_orders must have exactly 1 order"
    );
    assert!(my_orders.result.taker_orders.is_empty(), "taker_orders must be empty");
    let maker_order = my_orders.result.maker_orders.get(&response.result.uuid).unwrap();

    let expected_min_volume: BigDecimal = "0.2".parse().unwrap();
    assert_eq!(expected_min_volume, maker_order.min_base_vol);
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_best_orders() {
    let bob_passphrase = get_passphrase(&".env.seed", "BOB_PASSPHRASE").unwrap();

    let coins = json!([
        {"coin":"RICK","asset":"RICK","rpcport":8923,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","rpcport":11608,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"},"rpcport":80},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20", "protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}}}
    ]);

    // start bob and immediately place the orders
    let mut mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": bob_passphrase,
            "coins": coins,
            "rpc_password": "pass",
            "i_am_seed": true,
        }),
        "pass".into(),
        local_start!("bob"),
    )
    .unwrap();
    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log!({"Bob log path: {}", mm_bob.log_path.display()});

    // Enable coins on Bob side. Print the replies in case we need the "address".
    let bob_coins = block_on(enable_coins_eth_electrum(&mm_bob, &["http://195.201.0.6:8565"]));
    log!({ "enable_coins (bob): {:?}", bob_coins });
    // issue sell request on Bob side by setting base/rel price
    log!("Issue bob sell requests");

    let bob_orders = [
        // (base, rel, price, volume, min_volume)
        ("RICK", "MORTY", "0.9", "0.9", None),
        ("RICK", "MORTY", "0.8", "0.9", None),
        ("RICK", "MORTY", "0.7", "0.9", Some("0.9")),
        ("RICK", "ETH", "0.8", "0.9", None),
        ("MORTY", "RICK", "0.8", "0.9", None),
        ("MORTY", "RICK", "0.9", "0.9", None),
        ("ETH", "RICK", "0.8", "0.9", None),
        ("MORTY", "ETH", "0.8", "0.8", None),
        ("MORTY", "ETH", "0.7", "0.8", Some("0.8")),
    ];
    for (base, rel, price, volume, min_volume) in bob_orders.iter() {
        let rc = block_on(mm_bob.rpc(json! ({
            "userpass": mm_bob.userpass,
            "method": "setprice",
            "base": base,
            "rel": rel,
            "price": price,
            "volume": volume,
            "min_volume": min_volume.unwrap_or("0.00777"),
            "cancel_previous": false,
        })))
        .unwrap();
        assert!(rc.0.is_success(), "!setprice: {}", rc.1);
    }

    let mm_alice = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("ALICE_TRADE_IP") .ok(),
            "rpcip": env::var ("ALICE_TRADE_IP") .ok(),
            "passphrase": "alice passphrase",
            "coins": coins,
            "seednodes": [fomat!((mm_bob.ip))],
            "rpc_password": "pass",
        }),
        "pass".into(),
        local_start!("alice"),
    )
    .unwrap();

    let (_alice_dump_log, _alice_dump_dashboard) = mm_alice.mm_dump();
    log!({ "Alice log path: {}", mm_alice.log_path.display() });

    block_on(mm_bob.wait_for_log(22., |log| {
        log.contains("DEBUG Handling IncludedTorelaysMesh message for peer")
    }))
    .unwrap();

    let rc = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "method": "best_orders",
        "coin": "RICK",
        "action": "buy",
        "volume": "0.1",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!best_orders: {}", rc.1);
    let response: BestOrdersResponse = json::from_str(&rc.1).unwrap();
    let best_morty_orders = response.result.get("MORTY").unwrap();
    assert_eq!(1, best_morty_orders.len());
    let expected_price: BigDecimal = "0.8".parse().unwrap();
    assert_eq!(expected_price, best_morty_orders[0].price);

    let rc = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "method": "best_orders",
        "coin": "RICK",
        "action": "buy",
        "volume": "1.7",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!best_orders: {}", rc.1);
    let response: BestOrdersResponse = json::from_str(&rc.1).unwrap();
    // MORTY
    let best_morty_orders = response.result.get("MORTY").unwrap();
    let expected_price: BigDecimal = "0.7".parse().unwrap();
    let bob_morty_addr = addr_from_enable(&bob_coins, "MORTY");
    assert_eq!(expected_price, best_morty_orders[0].price);
    assert_eq!(bob_morty_addr, best_morty_orders[0].address);
    let expected_price: BigDecimal = "0.8".parse().unwrap();
    assert_eq!(expected_price, best_morty_orders[1].price);
    assert_eq!(bob_morty_addr, best_morty_orders[1].address);
    // ETH
    let expected_price: BigDecimal = "0.8".parse().unwrap();
    let best_eth_orders = response.result.get("ETH").unwrap();
    assert_eq!(expected_price, best_eth_orders[0].price);

    let rc = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "method": "best_orders",
        "coin": "RICK",
        "action": "sell",
        "volume": "0.1",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!best_orders: {}", rc.1);
    let response: BestOrdersResponse = json::from_str(&rc.1).unwrap();

    let expected_price: BigDecimal = "1.25".parse().unwrap();

    let best_morty_orders = response.result.get("MORTY").unwrap();
    assert_eq!(expected_price, best_morty_orders[0].price);
    assert_eq!(1, best_morty_orders.len());

    let best_eth_orders = response.result.get("ETH").unwrap();
    assert_eq!(expected_price, best_eth_orders[0].price);

    let rc = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "method": "best_orders",
        "coin": "ETH",
        "action": "sell",
        "volume": "0.1",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!best_orders: {}", rc.1);
    let response: BestOrdersResponse = json::from_str(&rc.1).unwrap();

    let expected_price: BigDecimal = "1.25".parse().unwrap();

    let best_morty_orders = response.result.get("MORTY").unwrap();
    assert_eq!(expected_price, best_morty_orders[0].price);
    assert_eq!("MORTY", best_morty_orders[0].coin);
    assert_eq!(1, best_morty_orders.len());

    block_on(mm_bob.stop()).unwrap();
    block_on(mm_alice.stop()).unwrap();
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_best_orders_duplicates_after_update() {
    let eve_passphrase = get_passphrase(&".env.seed", "BOB_PASSPHRASE").unwrap();

    let coins = json!([
        {"coin":"RICK","asset":"RICK","rpcport":8923,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","rpcport":11608,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"},"rpcport":80},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20", "protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}}}
    ]);

    // start bob as a seednode
    let mut mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": "bob",
            "coins": coins,
            "rpc_password": "pass",
            "i_am_seed": true,
        }),
        "pass".into(),
        local_start!("bob"),
    )
    .unwrap();

    // start eve and immediately place the order
    let mm_eve = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": eve_passphrase,
            "coins": coins,
            "rpc_password": "pass",
            "seednodes": [fomat!((mm_bob.ip))],
        }),
        "pass".into(),
        local_start!("bob"),
    )
    .unwrap();
    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log!({"Bob log path: {}", mm_bob.log_path.display()});

    // Enable coins on Eve side. Print the replies in case we need the "address".
    let eve_coins = block_on(enable_coins_eth_electrum(&mm_eve, &["http://195.201.0.6:8565"]));
    log!({ "enable_coins (eve): {:?}", eve_coins });
    // issue sell request on Eve side by setting base/rel price
    log!("Issue eve sell request");

    let rc = block_on(mm_eve.rpc(json! ({
        "userpass": mm_eve.userpass,
        "method": "setprice",
        "base": "RICK",
        "rel": "MORTY",
        "price": "1",
        "volume": "1",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!setprice: {}", rc.1);
    let eve_order: SetPriceResponse = json::from_str(&rc.1).unwrap();

    let mm_alice = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("ALICE_TRADE_IP") .ok(),
            "rpcip": env::var ("ALICE_TRADE_IP") .ok(),
            "passphrase": "alice passphrase",
            "coins": coins,
            "seednodes": [fomat!((mm_bob.ip))],
            "rpc_password": "pass",
        }),
        "pass".into(),
        local_start!("alice"),
    )
    .unwrap();

    let (_alice_dump_log, _alice_dump_dashboard) = mm_alice.mm_dump();
    log!({ "Alice log path: {}", mm_alice.log_path.display() });

    block_on(mm_bob.wait_for_log(22., |log| {
        log.contains("DEBUG Handling IncludedTorelaysMesh message for peer")
    }))
    .unwrap();

    let rc = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "method": "best_orders",
        "coin": "RICK",
        "action": "buy",
        "volume": "0.1",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!best_orders: {}", rc.1);
    let response: BestOrdersResponse = json::from_str(&rc.1).unwrap();
    let best_morty_orders = response.result.get("MORTY").unwrap();
    assert_eq!(1, best_morty_orders.len());
    let expected_price: BigDecimal = "1".parse().unwrap();
    assert_eq!(expected_price, best_morty_orders[0].price);

    for _ in 0..5 {
        let rc = block_on(mm_eve.rpc(json!({
            "userpass": mm_eve.userpass,
            "method": "update_maker_order",
            "uuid": eve_order.result.uuid,
            "new_price": "1.1",
        })))
        .unwrap();
        assert!(rc.0.is_success(), "!setprice: {}", rc.1);
        thread::sleep(Duration::from_secs(1));
    }

    for _ in 0..5 {
        let rc = block_on(mm_eve.rpc(json!({
            "userpass": mm_eve.userpass,
            "method": "update_maker_order",
            "uuid": eve_order.result.uuid,
            "new_price": "1.2",
        })))
        .unwrap();
        assert!(rc.0.is_success(), "!setprice: {}", rc.1);
        thread::sleep(Duration::from_secs(1));
    }

    let rc = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "method": "best_orders",
        "coin": "RICK",
        "action": "buy",
        "volume": "500",
    })))
    .unwrap();

    assert!(rc.0.is_success(), "!best_orders: {}", rc.1);
    let response: BestOrdersResponse = json::from_str(&rc.1).unwrap();
    let best_morty_orders = response.result.get("MORTY").unwrap();
    assert_eq!(1, best_morty_orders.len());
    let expected_price: BigDecimal = "1.2".parse().unwrap();
    assert_eq!(expected_price, best_morty_orders[0].price);

    block_on(mm_bob.stop()).unwrap();
    block_on(mm_alice.stop()).unwrap();
    block_on(mm_eve.stop()).unwrap();
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_best_orders_filter_response() {
    let bob_passphrase = get_passphrase(&".env.seed", "BOB_PASSPHRASE").unwrap();

    let bob_coins_config = json!([
        {"coin":"RICK","asset":"RICK","rpcport":8923,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","rpcport":11608,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"},"rpcport":80},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20", "protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}}}
    ]);

    // alice defined MORTY as "wallet_only" in config
    let alice_coins_config = json!([
        {"coin":"RICK","asset":"RICK","rpcport":8923,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","rpcport":11608,"wallet_only": true,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"},"rpcport":80},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20", "protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}}}
    ]);

    // start bob and immediately place the orders
    let mut mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": bob_passphrase,
            "coins": bob_coins_config,
            "rpc_password": "pass",
            "i_am_seed": true,
        }),
        "pass".into(),
        local_start!("bob"),
    )
    .unwrap();
    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log!({"Bob log path: {}", mm_bob.log_path.display()});

    // Enable coins on Bob side. Print the replies in case we need the "address".
    let bob_coins = block_on(enable_coins_eth_electrum(&mm_bob, &["http://195.201.0.6:8565"]));
    log!({ "enable_coins (bob): {:?}", bob_coins });
    // issue sell request on Bob side by setting base/rel price
    log!("Issue bob sell requests");

    let bob_orders = [
        // (base, rel, price, volume, min_volume)
        ("RICK", "MORTY", "0.9", "0.9", None),
        ("RICK", "MORTY", "0.8", "0.9", None),
        ("RICK", "MORTY", "0.7", "0.9", Some("0.9")),
        ("RICK", "ETH", "0.8", "0.9", None),
        ("MORTY", "RICK", "0.8", "0.9", None),
        ("MORTY", "RICK", "0.9", "0.9", None),
        ("ETH", "RICK", "0.8", "0.9", None),
        ("MORTY", "ETH", "0.8", "0.8", None),
        ("MORTY", "ETH", "0.7", "0.8", Some("0.8")),
    ];
    for (base, rel, price, volume, min_volume) in bob_orders.iter() {
        let rc = block_on(mm_bob.rpc(json! ({
            "userpass": mm_bob.userpass,
            "method": "setprice",
            "base": base,
            "rel": rel,
            "price": price,
            "volume": volume,
            "min_volume": min_volume.unwrap_or("0.00777"),
            "cancel_previous": false,
        })))
        .unwrap();
        assert!(rc.0.is_success(), "!setprice: {}", rc.1);
    }

    let mm_alice = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("ALICE_TRADE_IP") .ok(),
            "rpcip": env::var ("ALICE_TRADE_IP") .ok(),
            "passphrase": "alice passphrase",
            "coins": alice_coins_config,
            "seednodes": [fomat!((mm_bob.ip))],
            "rpc_password": "pass",
        }),
        "pass".into(),
        local_start!("alice"),
    )
    .unwrap();

    let (_alice_dump_log, _alice_dump_dashboard) = mm_alice.mm_dump();
    log!({ "Alice log path: {}", mm_alice.log_path.display() });

    block_on(mm_bob.wait_for_log(22., |log| {
        log.contains("DEBUG Handling IncludedTorelaysMesh message for peer")
    }))
    .unwrap();

    let rc = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "method": "best_orders",
        "coin": "RICK",
        "action": "buy",
        "volume": "0.1",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!best_orders: {}", rc.1);
    let response: BestOrdersResponse = json::from_str(&rc.1).unwrap();
    let empty_vec = Vec::new();
    let best_morty_orders = response.result.get("MORTY").unwrap_or(&empty_vec);
    assert_eq!(0, best_morty_orders.len());
    let best_eth_orders = response.result.get("ETH").unwrap();
    assert_eq!(1, best_eth_orders.len());

    block_on(mm_bob.stop()).unwrap();
    block_on(mm_alice.stop()).unwrap();
}

fn request_and_check_orderbook_depth(mm_alice: &MarketMakerIt) {
    let rc = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "method": "orderbook_depth",
        "pairs": [("RICK", "MORTY"), ("RICK", "ETH"), ("MORTY", "ETH")],
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!orderbook_depth: {}", rc.1);
    let response: OrderbookDepthResponse = json::from_str(&rc.1).unwrap();
    let rick_morty = response
        .result
        .iter()
        .find(|pair_depth| pair_depth.pair.0 == "RICK" && pair_depth.pair.1 == "MORTY")
        .unwrap();
    assert_eq!(3, rick_morty.depth.asks);
    assert_eq!(2, rick_morty.depth.bids);

    let rick_eth = response
        .result
        .iter()
        .find(|pair_depth| pair_depth.pair.0 == "RICK" && pair_depth.pair.1 == "ETH")
        .unwrap();
    assert_eq!(1, rick_eth.depth.asks);
    assert_eq!(1, rick_eth.depth.bids);

    let morty_eth = response
        .result
        .iter()
        .find(|pair_depth| pair_depth.pair.0 == "MORTY" && pair_depth.pair.1 == "ETH")
        .unwrap();
    assert_eq!(0, morty_eth.depth.asks);
    assert_eq!(0, morty_eth.depth.bids);
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_orderbook_depth() {
    let bob_passphrase = get_passphrase(&".env.seed", "BOB_PASSPHRASE").unwrap();

    let coins = json!([
        {"coin":"RICK","asset":"RICK","rpcport":8923,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","rpcport":11608,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"},"rpcport":80},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20", "protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}}}
    ]);

    // start bob and immediately place the orders
    let mut mm_bob = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": bob_passphrase,
            "coins": coins,
            "rpc_password": "pass",
            "i_am_seed": true,
        }),
        "pass".into(),
        local_start!("bob"),
    )
    .unwrap();
    let (_bob_dump_log, _bob_dump_dashboard) = mm_bob.mm_dump();
    log!({"Bob log path: {}", mm_bob.log_path.display()});

    // Enable coins on Bob side. Print the replies in case we need the "address".
    let bob_coins = block_on(enable_coins_eth_electrum(&mm_bob, &["http://195.201.0.6:8565"]));
    log!({ "enable_coins (bob): {:?}", bob_coins });
    // issue sell request on Bob side by setting base/rel price
    log!("Issue bob sell requests");

    let bob_orders = [
        // (base, rel, price, volume, min_volume)
        ("RICK", "MORTY", "0.9", "0.9", None),
        ("RICK", "MORTY", "0.8", "0.9", None),
        ("RICK", "MORTY", "0.7", "0.9", Some("0.9")),
        ("RICK", "ETH", "0.8", "0.9", None),
        ("MORTY", "RICK", "0.8", "0.9", None),
        ("MORTY", "RICK", "0.9", "0.9", None),
        ("ETH", "RICK", "0.8", "0.9", None),
    ];
    for (base, rel, price, volume, min_volume) in bob_orders.iter() {
        let rc = block_on(mm_bob.rpc(json! ({
            "userpass": mm_bob.userpass,
            "method": "setprice",
            "base": base,
            "rel": rel,
            "price": price,
            "volume": volume,
            "min_volume": min_volume.unwrap_or("0.00777"),
            "cancel_previous": false,
        })))
        .unwrap();
        assert!(rc.0.is_success(), "!setprice: {}", rc.1);
    }

    let mm_alice = MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 9998,
            "myipaddr": env::var ("ALICE_TRADE_IP") .ok(),
            "rpcip": env::var ("ALICE_TRADE_IP") .ok(),
            "passphrase": "alice passphrase",
            "coins": coins,
            "seednodes": [fomat!((mm_bob.ip))],
            "rpc_password": "pass",
        }),
        "pass".into(),
        local_start!("alice"),
    )
    .unwrap();

    let (_alice_dump_log, _alice_dump_dashboard) = mm_alice.mm_dump();
    log!({ "Alice log path: {}", mm_alice.log_path.display() });

    block_on(mm_bob.wait_for_log(22., |log| {
        log.contains("DEBUG Handling IncludedTorelaysMesh message for peer")
    }))
    .unwrap();

    request_and_check_orderbook_depth(&mm_alice);
    // request RICK/MORTY orderbook to subscribe Alice
    let rc = block_on(mm_alice.rpc(json! ({
        "userpass": mm_alice.userpass,
        "method": "orderbook",
        "base": "RICK",
        "rel": "MORTY",
    })))
    .unwrap();
    assert!(rc.0.is_success(), "!orderbook: {}", rc.1);

    request_and_check_orderbook_depth(&mm_alice);

    block_on(mm_bob.stop()).unwrap();
    block_on(mm_alice.stop()).unwrap();
}

// https://github.com/KomodoPlatform/atomicDEX-API/issues/932
#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_mm2_db_migration() {
    let bob_passphrase = get_passphrase(&".env.client", "BOB_PASSPHRASE").unwrap();

    let coins = json! ([
        {"coin":"RICK","asset":"RICK","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"MORTY","asset":"MORTY","required_confirmations":0,"txversion":4,"overwintered":1,"protocol":{"type":"UTXO"}},
        {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"}},
        {"coin":"JST","name":"jst","protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}}}
    ]);

    let mm2_folder = new_mm2_temp_folder_path(None);
    let swaps_dir = mm2_folder.join(format!(
        "{}/SWAPS/STATS/MAKER",
        hex::encode(rmd160_from_passphrase(&bob_passphrase))
    ));
    std::fs::create_dir_all(&swaps_dir).unwrap();
    let swap_path = swaps_dir.join("5d02843e-d1b4-488d-aad0-114d82020453.json");
    let swap_json = r#"{"uuid":"5d02843e-d1b4-488d-aad0-114d82020453","events":[{"timestamp":1612780908136,"event":{"type":"Started","data":{"taker_coin":"MORTY-BEP20","maker_coin":"RICK-BEP20","taker":"ad6f89abc2e5beaa8a3ac28e22170659b3209fe2ddf439681b4b8f31508c36fa","secret":"0000000000000000000000000000000000000000000000000000000000000000","secret_hash":"026bebc2e19c243d0940dd583c9573bf10377afd","my_persistent_pub":"037310a8fb9fd8f198a1a21db830252ad681fccda580ed4101f3f6bfb98b34fab5","lock_duration":7800,"maker_amount":"1","taker_amount":"1","maker_payment_confirmations":1,"maker_payment_requires_nota":false,"taker_payment_confirmations":1,"taker_payment_requires_nota":false,"maker_payment_lock":1612796508,"uuid":"5d02843e-d1b4-488d-aad0-114d82020453","started_at":1612780908,"maker_coin_start_block":793472,"taker_coin_start_block":797356,"maker_payment_trade_fee":null,"taker_payment_spend_trade_fee":null}}},{"timestamp":1612780924142,"event":{"type":"Negotiated","data":{"taker_payment_locktime":1612788708,"taker_pubkey":"03ad6f89abc2e5beaa8a3ac28e22170659b3209fe2ddf439681b4b8f31508c36fa"}}},{"timestamp":1612780935156,"event":{"type":"TakerFeeValidated","data":{"tx_hex":"0400008085202f8901f425fbefe21f33ccb7b487df251191b27dfa7b639b04f60e5493c7ea41dbf149000000006b483045022100d5ec3e542175479bd4bd011e19b76a75e99f19cc49867e5bca9541950322c33a02207a4d1ffd674fb9760de79bb4929af44d66344b5e182de3c377186deebf6bf376012103ad6f89abc2e5beaa8a3ac28e22170659b3209fe2ddf439681b4b8f31508c36faffffffff02bcf60100000000001976a914ca1e04745e8ca0c60d8c5881531d51bec470743f88ac5ce6f305000000001976a914d55f0df6cb82630ad21a4e6049522a6f2b6c9d4588ac7c152160000000000000000000000000000000","tx_hash":"75323ab7acd64bd35242611fabaec560d9acf2e1f9ca28d3a4aba47a79fb49c4"}}},{"timestamp":1612780935174,"event":{"type":"MakerPaymentSent","data":{"tx_hex":"0400008085202f89028bef955e42107c562e4e02421f25c455723a701573f86c17b4d82e35a7d8f9f7020000006b483045022100b12fc9d95acca76bf5fd8d5c6acc288b454032ba4561b1c2b1f5f33b2cf2926d022017e561bc2cd93308848674b47b2e8ebd8f074ea78e32454d5fea6f08c0b1f1e40121037310a8fb9fd8f198a1a21db830252ad681fccda580ed4101f3f6bfb98b34fab5ffffffff5dfd0b24c0f7c3cf235868cf9a26ec49574764d135796fc4e7d20e95d55a8653000000006a47304402207c752d14601d1c99892f9d6c88c8ff2f93211640a65b2ee69172a16b908b21e402206f0b66684158445888271a849ab46258ad722496ee64fde055a6f44e36ed2ccc0121037310a8fb9fd8f198a1a21db830252ad681fccda580ed4101f3f6bfb98b34fab5ffffffff0300e1f5050000000017a9141b85c1a277f44f7d77d52b78e2ba70a0becc2ff9870000000000000000166a14026bebc2e19c243d0940dd583c9573bf10377afda7d26301000000001976a91486f747b28c60ad1130bdd3f84f48eeaf1801ca9888ac87152160000000000000000000000000000000","tx_hash":"27dafe553246553d54f909fbbded80e6d490fdb95ca7b6807d73eca45f0d7a22"}}},{"timestamp":1612780982221,"event":{"type":"TakerPaymentReceived","data":{"tx_hex":"0400008085202f8902c449fb797aa4aba4d328caf9e1f2acd960c5aeab1f614252d34bd6acb73a3275010000006a47304402200438c96bf457bacf906e94c98f91783129cb1c3a8f3d9355e1c39a9857fb2c6b02201d3c71b3f243f7a3c91bb9a15e80bb26e47bed04e798106a8af8dac61082ec41012103ad6f89abc2e5beaa8a3ac28e22170659b3209fe2ddf439681b4b8f31508c36fafffffffff425fbefe21f33ccb7b487df251191b27dfa7b639b04f60e5493c7ea41dbf149010000006b483045022100efa00c742159b0b05433678aa95f0c8900adaddf5011bfaf56d6a7679aed428b022043f68efc3cb386dd10a65a2a3e8a904541c8f1ddbd7dddbcda2ccdd7938c5934012103ad6f89abc2e5beaa8a3ac28e22170659b3209fe2ddf439681b4b8f31508c36faffffffff0300e1f5050000000017a914bc8e8f2648f7bb4dbd612f2e71dd7b23c54880b7870000000000000000166a14026bebc2e19c243d0940dd583c9573bf10377afd74c3e90b000000001976a914d55f0df6cb82630ad21a4e6049522a6f2b6c9d4588acb5152160000000000000000000000000000000","tx_hash":"94c8a1244421465b618a36e7647a270c7b2ef20eff3cd1317761cc242c49cc99"}}},{"timestamp":1612780982222,"event":{"type":"TakerPaymentWaitConfirmStarted"}},{"timestamp":1612781042265,"event":{"type":"TakerPaymentValidatedAndConfirmed"}},{"timestamp":1612781042272,"event":{"type":"TakerPaymentSpent","data":{"tx_hex":"0400008085202f890199cc492c24cc617731d13cff0ef22e7b0c277a64e7368a615b46214424a1c89400000000d84830450221008f38d29e7990bd694f2c4fd4c235fe00997da4e5133208d7c38e75e806d9be1702201ff1d598ceafc099dc4af7d4b91db535196f642cf31b5b5e386b28574a378b9b0120e8512e2afb02d3a90590d30095286e2293f51f9d4411ad87ef398ee8f566de43004c6b6304e4332160b1752103ad6f89abc2e5beaa8a3ac28e22170659b3209fe2ddf439681b4b8f31508c36faac6782012088a914026bebc2e19c243d0940dd583c9573bf10377afd8821037310a8fb9fd8f198a1a21db830252ad681fccda580ed4101f3f6bfb98b34fab5ac68ffffffff0118ddf505000000001976a91486f747b28c60ad1130bdd3f84f48eeaf1801ca9888ace2072160000000000000000000000000000000","tx_hash":"d21173cca32b83ffe5d4cc327f7eff09496f52876614dbbfe7963284818ba9a1"}}},{"timestamp":1612781042273,"event":{"type":"TakerPaymentSpendConfirmStarted"}},{"timestamp":1612781207356,"event":{"type":"TakerPaymentSpendConfirmed"}},{"timestamp":1612781207357,"event":{"type":"Finished"}}],"maker_amount":"1","maker_coin":"RICK-BEP20","taker_amount":"1","taker_coin":"MORTY-BEP20","gui":"dexstats","mm_version":"19701cc87","success_events":["Started","Negotiated","TakerFeeValidated","MakerPaymentSent","TakerPaymentReceived","TakerPaymentWaitConfirmStarted","TakerPaymentValidatedAndConfirmed","TakerPaymentSpent","TakerPaymentSpendConfirmStarted","TakerPaymentSpendConfirmed","Finished"],"error_events":["StartFailed","NegotiateFailed","TakerFeeValidateFailed","MakerPaymentTransactionFailed","MakerPaymentDataSendFailed","MakerPaymentWaitConfirmFailed","TakerPaymentValidateFailed","TakerPaymentWaitConfirmFailed","TakerPaymentSpendFailed","TakerPaymentSpendConfirmFailed","MakerPaymentWaitRefundStarted","MakerPaymentRefunded","MakerPaymentRefundFailed"]}"#;
    std::fs::write(swap_path, swap_json.as_bytes()).unwrap();

    // if there is an issue with migration the start will fail
    MarketMakerIt::start(
        json! ({
            "gui": "nogui",
            "netid": 8999,
            "dht": "on",  // Enable DHT without delay.
            "myipaddr": env::var ("BOB_TRADE_IP") .ok(),
            "rpcip": env::var ("BOB_TRADE_IP") .ok(),
            "canbind": env::var ("BOB_TRADE_PORT") .ok().map (|s| s.parse::<i64>().unwrap()),
            "passphrase": bob_passphrase,
            "coins": coins,
            "rpc_password": "password",
            "i_am_seed": true,
            "dbdir": mm2_folder.display().to_string(),
        }),
        "password".into(),
        None,
    )
    .unwrap();
}
