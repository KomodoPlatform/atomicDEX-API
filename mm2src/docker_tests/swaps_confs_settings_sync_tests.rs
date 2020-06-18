use crate::mm2::lp_ordermatch::OrderConfirmationsSettings;
use super::*;

struct SwapConfirmationsSettings {
    maker_coin_confs: u64,
    maker_coin_nota: bool,
    taker_coin_confs: u64,
    taker_coin_nota: bool,
}

fn test_confirmation_settings_sync_correctly_on_buy(
    maker_settings: OrderConfirmationsSettings,
    taker_settings: OrderConfirmationsSettings,
    expected_maker: SwapConfirmationsSettings,
    expected_taker: SwapConfirmationsSettings,
) {
    let (_ctx, _, bob_priv_key) = generate_coin_with_random_privkey("MYCOIN", 1000);
    let (_ctx, _, alice_priv_key) = generate_coin_with_random_privkey("MYCOIN1", 2000);
    let coins = json! ([
        {"coin":"MYCOIN","asset":"MYCOIN","txversion":4,"overwintered":1,"txfee":1000},
        {"coin":"MYCOIN1","asset":"MYCOIN1","txversion":4,"overwintered":1,"txfee":1000},
    ]);
    let mut mm_bob = unwrap! (MarketMakerIt::start (
        json! ({
            "gui": "nogui",
            "netid": 9000,
            "dht": "on",  // Enable DHT without delay.
            "passphrase": format!("0x{}", hex::encode(bob_priv_key)),
            "coins": coins,
            "rpc_password": "pass",
            "i_am_seed": true,
        }),
        "pass".to_string(),
        None,
    ));
    let (_bob_dump_log, _bob_dump_dashboard) = mm_dump (&mm_bob.log_path);
    unwrap! (block_on (mm_bob.wait_for_log (22., |log| log.contains (">>>>>>>>> DEX stats "))));

    let mut mm_alice = unwrap! (MarketMakerIt::start (
        json! ({
            "gui": "nogui",
            "netid": 9000,
            "dht": "on",  // Enable DHT without delay.
            "passphrase": format!("0x{}", hex::encode(alice_priv_key)),
            "coins": coins,
            "rpc_password": "pass",
            "seednodes": vec![format!("{}", mm_bob.ip)],
        }),
        "pass".to_string(),
        None,
    ));
    let (_alice_dump_log, _alice_dump_dashboard) = mm_dump (&mm_alice.log_path);
    unwrap! (block_on (mm_alice.wait_for_log (22., |log| log.contains (">>>>>>>>> DEX stats "))));

    log!([block_on(enable_native(&mm_bob, "MYCOIN", vec![]))]);
    log!([block_on(enable_native(&mm_bob, "MYCOIN1", vec![]))]);
    log!([block_on(enable_native(&mm_alice, "MYCOIN", vec![]))]);
    log!([block_on(enable_native(&mm_alice, "MYCOIN1", vec![]))]);
    let rc = unwrap! (block_on (mm_bob.rpc (json! ({
        "userpass": mm_bob.userpass,
        "method": "setprice",
        "base": "MYCOIN",
        "rel": "MYCOIN1",
        "price": 1,
        "volume": 1,
        "base_confs": maker_settings.base_confs,
        "base_nota": maker_settings.base_nota,
        "rel_confs": maker_settings.rel_confs,
        "rel_nota": maker_settings.rel_nota,
    }))));
    assert! (rc.0.is_success(), "!setprice: {}", rc.1);
    log!("Maker order " (rc.1));

    let rc = unwrap! (block_on (mm_alice.rpc (json! ({
        "userpass": mm_alice.userpass,
        "method": "buy",
        "base": "MYCOIN",
        "rel": "MYCOIN1",
        "price": 1,
        "volume": "0.5",
        "base_confs": taker_settings.base_confs,
        "base_nota": taker_settings.base_nota,
        "rel_confs": taker_settings.rel_confs,
        "rel_nota": taker_settings.rel_nota,
    }))));
    assert! (rc.0.is_success(), "!buy: {}", rc.1);
    let rc_json: Json = json::from_str(&rc.1).unwrap();
    let uuid = &rc_json["result"]["uuid"];

    unwrap! (block_on (mm_bob.wait_for_log (22., |log| log.contains ("Entering the maker_swap_loop MYCOIN/MYCOIN1"))));
    unwrap! (block_on (mm_alice.wait_for_log (22., |log| log.contains ("Entering the taker_swap_loop MYCOIN/MYCOIN1"))));
    log!("Sleep for 3 seconds to allow Started event to be saved");
    thread::sleep(Duration::from_secs(3));

    let maker_status = unwrap! (block_on(mm_bob.rpc (json! ({
        "userpass": mm_bob.userpass,
        "method": "my_swap_status",
        "params": {
            "uuid": uuid,
        }
    }))));
    assert!(maker_status.0.is_success(), "!maker_status of {}: {}", uuid, maker_status.1);
    let maker_status_json: Json = json::from_str(&maker_status.1).unwrap();
    let maker_started_event = maker_status_json["result"]["events"].as_array().unwrap()[0].clone();
    assert_eq!(maker_started_event["event"]["data"]["maker_payment_confirmations"].as_u64(), Some(expected_maker.maker_coin_confs));
    assert_eq!(maker_started_event["event"]["data"]["maker_payment_requires_nota"].as_bool(), Some(expected_maker.maker_coin_nota));
    assert_eq!(maker_started_event["event"]["data"]["taker_payment_confirmations"].as_u64(), Some(expected_maker.taker_coin_confs));
    assert_eq!(maker_started_event["event"]["data"]["taker_payment_requires_nota"].as_bool(), Some(expected_maker.taker_coin_nota));

    let taker_status = unwrap! (block_on(mm_alice.rpc (json! ({
        "userpass": mm_alice.userpass,
        "method": "my_swap_status",
        "params": {
            "uuid": uuid,
        }
    }))));
    assert!(taker_status.0.is_success(), "!taker_status of {}: {}", uuid, taker_status.1);
    let maker_status_json: Json = json::from_str(&taker_status.1).unwrap();
    let maker_started_event = maker_status_json["result"]["events"].as_array().unwrap()[0].clone();
    assert_eq!(maker_started_event["event"]["data"]["maker_payment_confirmations"].as_u64(), Some(expected_taker.maker_coin_confs));
    assert_eq!(maker_started_event["event"]["data"]["maker_payment_requires_nota"].as_bool(), Some(expected_taker.maker_coin_nota));
    assert_eq!(maker_started_event["event"]["data"]["taker_payment_confirmations"].as_u64(), Some(expected_taker.taker_coin_confs));
    assert_eq!(maker_started_event["event"]["data"]["taker_payment_requires_nota"].as_bool(), Some(expected_taker.taker_coin_nota));

    unwrap!(block_on(mm_bob.stop()));
    unwrap!(block_on(mm_alice.stop()));
}

fn test_confirmation_settings_sync_correctly_on_sell(
    maker_settings: OrderConfirmationsSettings,
    taker_settings: OrderConfirmationsSettings,
    expected_maker: SwapConfirmationsSettings,
    expected_taker: SwapConfirmationsSettings,
) {
    let (_ctx, _, bob_priv_key) = generate_coin_with_random_privkey("MYCOIN", 1000);
    let (_ctx, _, alice_priv_key) = generate_coin_with_random_privkey("MYCOIN1", 2000);
    let coins = json! ([
        {"coin":"MYCOIN","asset":"MYCOIN","txversion":4,"overwintered":1,"txfee":1000},
        {"coin":"MYCOIN1","asset":"MYCOIN1","txversion":4,"overwintered":1,"txfee":1000},
    ]);
    let mut mm_bob = unwrap! (MarketMakerIt::start (
        json! ({
            "gui": "nogui",
            "netid": 9000,
            "dht": "on",  // Enable DHT without delay.
            "passphrase": format!("0x{}", hex::encode(bob_priv_key)),
            "coins": coins,
            "rpc_password": "pass",
            "i_am_seed": true,
        }),
        "pass".to_string(),
        None,
    ));
    let (_bob_dump_log, _bob_dump_dashboard) = mm_dump (&mm_bob.log_path);
    unwrap! (block_on (mm_bob.wait_for_log (22., |log| log.contains (">>>>>>>>> DEX stats "))));

    let mut mm_alice = unwrap! (MarketMakerIt::start (
        json! ({
            "gui": "nogui",
            "netid": 9000,
            "dht": "on",  // Enable DHT without delay.
            "passphrase": format!("0x{}", hex::encode(alice_priv_key)),
            "coins": coins,
            "rpc_password": "pass",
            "seednodes": vec![format!("{}", mm_bob.ip)],
        }),
        "pass".to_string(),
        None,
    ));
    let (_alice_dump_log, _alice_dump_dashboard) = mm_dump (&mm_alice.log_path);
    unwrap! (block_on (mm_alice.wait_for_log (22., |log| log.contains (">>>>>>>>> DEX stats "))));

    log!([block_on(enable_native(&mm_bob, "MYCOIN", vec![]))]);
    log!([block_on(enable_native(&mm_bob, "MYCOIN1", vec![]))]);
    log!([block_on(enable_native(&mm_alice, "MYCOIN", vec![]))]);
    log!([block_on(enable_native(&mm_alice, "MYCOIN1", vec![]))]);
    let rc = unwrap! (block_on (mm_bob.rpc (json! ({
        "userpass": mm_bob.userpass,
        "method": "setprice",
        "base": "MYCOIN",
        "rel": "MYCOIN1",
        "price": 1,
        "volume": 1,
        "base_confs": maker_settings.base_confs,
        "base_nota": maker_settings.base_nota,
        "rel_confs": maker_settings.rel_confs,
        "rel_nota": maker_settings.rel_nota,
    }))));
    assert! (rc.0.is_success(), "!setprice: {}", rc.1);
    log!("Maker order " (rc.1));

    let rc = unwrap! (block_on (mm_alice.rpc (json! ({
        "userpass": mm_alice.userpass,
        "method": "sell",
        "base": "MYCOIN1",
        "rel": "MYCOIN",
        "price": 1,
        "volume": "0.5",
        "base_confs": taker_settings.base_confs,
        "base_nota": taker_settings.base_nota,
        "rel_confs": taker_settings.rel_confs,
        "rel_nota": taker_settings.rel_nota,
    }))));
    assert! (rc.0.is_success(), "!buy: {}", rc.1);
    let rc_json: Json = json::from_str(&rc.1).unwrap();
    let uuid = &rc_json["result"]["uuid"];

    unwrap! (block_on (mm_bob.wait_for_log (22., |log| log.contains ("Entering the maker_swap_loop MYCOIN/MYCOIN1"))));
    unwrap! (block_on (mm_alice.wait_for_log (22., |log| log.contains ("Entering the taker_swap_loop MYCOIN/MYCOIN1"))));
    log!("Sleep for 3 seconds to allow Started event to be saved");
    thread::sleep(Duration::from_secs(3));

    let maker_status = unwrap! (block_on(mm_bob.rpc (json! ({
        "userpass": mm_bob.userpass,
        "method": "my_swap_status",
        "params": {
            "uuid": uuid,
        }
    }))));
    assert!(maker_status.0.is_success(), "!maker_status of {}: {}", uuid, maker_status.1);
    let maker_status_json: Json = json::from_str(&maker_status.1).unwrap();
    let maker_started_event = maker_status_json["result"]["events"].as_array().unwrap()[0].clone();
    assert_eq!(maker_started_event["event"]["data"]["maker_payment_confirmations"].as_u64(), Some(expected_maker.maker_coin_confs));
    assert_eq!(maker_started_event["event"]["data"]["maker_payment_requires_nota"].as_bool(), Some(expected_maker.maker_coin_nota));
    assert_eq!(maker_started_event["event"]["data"]["taker_payment_confirmations"].as_u64(), Some(expected_maker.taker_coin_confs));
    assert_eq!(maker_started_event["event"]["data"]["taker_payment_requires_nota"].as_bool(), Some(expected_maker.taker_coin_nota));

    let taker_status = unwrap! (block_on(mm_alice.rpc (json! ({
        "userpass": mm_alice.userpass,
        "method": "my_swap_status",
        "params": {
            "uuid": uuid,
        }
    }))));
    assert!(taker_status.0.is_success(), "!taker_status of {}: {}", uuid, taker_status.1);
    let maker_status_json: Json = json::from_str(&taker_status.1).unwrap();
    let maker_started_event = maker_status_json["result"]["events"].as_array().unwrap()[0].clone();
    assert_eq!(maker_started_event["event"]["data"]["maker_payment_confirmations"].as_u64(), Some(expected_taker.maker_coin_confs));
    assert_eq!(maker_started_event["event"]["data"]["maker_payment_requires_nota"].as_bool(), Some(expected_taker.maker_coin_nota));
    assert_eq!(maker_started_event["event"]["data"]["taker_payment_confirmations"].as_u64(), Some(expected_taker.taker_coin_confs));
    assert_eq!(maker_started_event["event"]["data"]["taker_payment_requires_nota"].as_bool(), Some(expected_taker.taker_coin_nota));

    unwrap!(block_on(mm_bob.stop()));
    unwrap!(block_on(mm_alice.stop()));
}

#[test]
fn test_buy_maker_should_use_taker_confs_and_notas_for_maker_payment_if_taker_requires_less() {
    let maker_settings = OrderConfirmationsSettings {
        base_confs: 2,
        base_nota: true,
        rel_confs: 2,
        rel_nota: true,
    };

    let taker_settings = OrderConfirmationsSettings {
        base_confs: 1,
        base_nota: false,
        rel_confs: 1,
        rel_nota: false,
    };

    let expected_maker = SwapConfirmationsSettings {
        maker_coin_confs: 1,
        maker_coin_nota: false,
        taker_coin_confs: 2,
        taker_coin_nota: true,
    };

    let expected_taker = SwapConfirmationsSettings {
        maker_coin_confs: 1,
        maker_coin_nota: false,
        taker_coin_confs: 1,
        taker_coin_nota: false,
    };

    test_confirmation_settings_sync_correctly_on_buy(
        maker_settings,
        taker_settings,
        expected_maker,
        expected_taker
    );
}

#[test]
fn test_buy_maker_should_not_use_taker_confs_and_notas_for_maker_payment_if_taker_requires_more() {
    let maker_settings = OrderConfirmationsSettings {
        base_confs: 1,
        base_nota: false,
        rel_confs: 2,
        rel_nota: true,
    };

    let taker_settings = OrderConfirmationsSettings {
        base_confs: 100,
        base_nota: true,
        rel_confs: 1,
        rel_nota: false,
    };

    let expected_maker = SwapConfirmationsSettings {
        maker_coin_confs: 1,
        maker_coin_nota: false,
        taker_coin_confs: 2,
        taker_coin_nota: true,
    };

    let expected_taker = SwapConfirmationsSettings {
        maker_coin_confs: 100,
        maker_coin_nota: true,
        taker_coin_confs: 1,
        taker_coin_nota: false,
    };

    test_confirmation_settings_sync_correctly_on_buy(
        maker_settings,
        taker_settings,
        expected_maker,
        expected_taker
    );
}

#[test]
fn test_buy_taker_should_use_maker_confs_and_notas_for_taker_payment_if_maker_requires_less() {
    let maker_settings = OrderConfirmationsSettings {
        base_confs: 1,
        base_nota: false,
        rel_confs: 1,
        rel_nota: false,
    };

    let taker_settings = OrderConfirmationsSettings {
        base_confs: 2,
        base_nota: true,
        rel_confs: 2,
        rel_nota: true,
    };

    let expected_maker = SwapConfirmationsSettings {
        maker_coin_confs: 1,
        maker_coin_nota: false,
        taker_coin_confs: 1,
        taker_coin_nota: false,
    };

    let expected_taker = SwapConfirmationsSettings {
        maker_coin_confs: 2,
        maker_coin_nota: true,
        taker_coin_confs: 1,
        taker_coin_nota: false,
    };

    test_confirmation_settings_sync_correctly_on_buy(
        maker_settings,
        taker_settings,
        expected_maker,
        expected_taker
    );
}

#[test]
fn test_buy_taker_should_not_use_maker_confs_and_notas_for_taker_payment_if_maker_requires_more() {
    let maker_settings = OrderConfirmationsSettings {
        base_confs: 1,
        base_nota: false,
        rel_confs: 100,
        rel_nota: true,
    };

    let taker_settings = OrderConfirmationsSettings {
        base_confs: 2,
        base_nota: true,
        rel_confs: 1,
        rel_nota: false,
    };

    let expected_maker = SwapConfirmationsSettings {
        maker_coin_confs: 1,
        maker_coin_nota: false,
        taker_coin_confs: 100,
        taker_coin_nota: true,
    };

    let expected_taker = SwapConfirmationsSettings {
        maker_coin_confs: 2,
        maker_coin_nota: true,
        taker_coin_confs: 1,
        taker_coin_nota: false,
    };

    test_confirmation_settings_sync_correctly_on_buy(
        maker_settings,
        taker_settings,
        expected_maker,
        expected_taker
    );
}

#[test]
fn test_sell_maker_should_use_taker_confs_and_notas_for_maker_payment_if_taker_requires_less() {
    let maker_settings = OrderConfirmationsSettings {
        base_confs: 2,
        base_nota: true,
        rel_confs: 2,
        rel_nota: true,
    };

    let taker_settings = OrderConfirmationsSettings {
        base_confs: 1,
        base_nota: false,
        rel_confs: 1,
        rel_nota: false,
    };

    let expected_maker = SwapConfirmationsSettings {
        maker_coin_confs: 1,
        maker_coin_nota: false,
        taker_coin_confs: 2,
        taker_coin_nota: true,
    };

    let expected_taker = SwapConfirmationsSettings {
        maker_coin_confs: 1,
        maker_coin_nota: false,
        taker_coin_confs: 1,
        taker_coin_nota: false,
    };

    test_confirmation_settings_sync_correctly_on_sell(
        maker_settings,
        taker_settings,
        expected_maker,
        expected_taker
    );
}

#[test]
fn test_sell_maker_should_not_use_taker_confs_and_notas_for_maker_payment_if_taker_requires_more() {
    let maker_settings = OrderConfirmationsSettings {
        base_confs: 1,
        base_nota: false,
        rel_confs: 2,
        rel_nota: true,
    };

    let taker_settings = OrderConfirmationsSettings {
        base_confs: 1,
        base_nota: false,
        rel_confs: 100,
        rel_nota: true,
    };

    let expected_maker = SwapConfirmationsSettings {
        maker_coin_confs: 1,
        maker_coin_nota: false,
        taker_coin_confs: 2,
        taker_coin_nota: true,
    };

    let expected_taker = SwapConfirmationsSettings {
        maker_coin_confs: 100,
        maker_coin_nota: true,
        taker_coin_confs: 1,
        taker_coin_nota: false,
    };

    test_confirmation_settings_sync_correctly_on_sell(
        maker_settings,
        taker_settings,
        expected_maker,
        expected_taker
    );
}

#[test]
fn test_sell_taker_should_use_maker_confs_and_notas_for_taker_payment_if_maker_requires_less() {
    let maker_settings = OrderConfirmationsSettings {
        base_confs: 1,
        base_nota: false,
        rel_confs: 1,
        rel_nota: false,
    };

    let taker_settings = OrderConfirmationsSettings {
        base_confs: 2,
        base_nota: true,
        rel_confs: 2,
        rel_nota: true,
    };

    let expected_maker = SwapConfirmationsSettings {
        maker_coin_confs: 1,
        maker_coin_nota: false,
        taker_coin_confs: 1,
        taker_coin_nota: false,
    };

    let expected_taker = SwapConfirmationsSettings {
        maker_coin_confs: 2,
        maker_coin_nota: true,
        taker_coin_confs: 1,
        taker_coin_nota: false,
    };

    test_confirmation_settings_sync_correctly_on_sell(
        maker_settings,
        taker_settings,
        expected_maker,
        expected_taker
    );
}

#[test]
fn test_sell_taker_should_not_use_maker_confs_and_notas_for_taker_payment_if_maker_requires_more() {
    let maker_settings = OrderConfirmationsSettings {
        base_confs: 1,
        base_nota: false,
        rel_confs: 100,
        rel_nota: true,
    };

    let taker_settings = OrderConfirmationsSettings {
        base_confs: 1,
        base_nota: false,
        rel_confs: 2,
        rel_nota: true,
    };

    let expected_maker = SwapConfirmationsSettings {
        maker_coin_confs: 1,
        maker_coin_nota: false,
        taker_coin_confs: 100,
        taker_coin_nota: true,
    };

    let expected_taker = SwapConfirmationsSettings {
        maker_coin_confs: 2,
        maker_coin_nota: true,
        taker_coin_confs: 1,
        taker_coin_nota: false,
    };

    test_confirmation_settings_sync_correctly_on_sell(
        maker_settings,
        taker_settings,
        expected_maker,
        expected_taker
    );
}
