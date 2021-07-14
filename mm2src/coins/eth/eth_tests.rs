use super::*;
use common::block_on;
use common::mm_ctx::{MmArc, MmCtxBuilder};
use mocktopus::mocking::*;

/// The gas price for the tests
const GAS_PRICE: u64 = 50_000_000_000;
// `GAS_PRICE` increased by 3%
const GAS_PRICE_APPROXIMATION_ON_START_SWAP: u64 = 51_500_000_000;
// `GAS_PRICE` increased by 5%
const GAS_PRICE_APPROXIMATION_ON_ORDER_ISSUE: u64 = 52_500_000_000;
// `GAS_PRICE` increased by 7%
const GAS_PRICE_APPROXIMATION_ON_TRADE_PREIMAGE: u64 = 53_500_000_000;

fn check_sum(addr: &str, expected: &str) {
    let actual = checksum_address(addr);
    assert_eq!(expected, actual);
}

fn eth_coin_for_test(
    coin_type: EthCoinType,
    urls: Vec<String>,
    fallback_swap_contract: Option<Address>,
) -> (MmArc, EthCoin) {
    let key_pair = KeyPair::from_secret_slice(
        &hex::decode("809465b17d0a4ddb3e4c69e8f23c2cabad868f51f8bed5c765ad1d6516c3306f").unwrap(),
    )
    .unwrap();
    let transport = Web3Transport::new(urls).unwrap();
    let web3 = Web3::new(transport);
    let conf = json!({
        "coins":[
           {"coin":"ETH","name":"ethereum","protocol":{"type":"ETH"},"rpcport":80,"mm2":1},
           {"coin":"JST","name":"jst","rpcport":80,"mm2":1,"protocol":{"type":"ERC20","protocol_data":{"platform":"ETH","contract_address":"0x2b294F029Fde858b2c62184e8390591755521d8E"}}}
        ]
    });
    let ctx = MmCtxBuilder::new().with_conf(conf.clone()).into_mm_arc();
    let ticker = match coin_type {
        EthCoinType::Eth => "ETH".to_string(),
        EthCoinType::Erc20 { .. } => "JST".to_string(),
    };

    let eth_coin = EthCoin(Arc::new(EthCoinImpl {
        coin_type,
        decimals: 18,
        gas_station_url: None,
        history_sync_state: Mutex::new(HistorySyncState::NotEnabled),
        my_address: key_pair.address(),
        key_pair,
        swap_contract_address: Address::from("0x7Bc1bBDD6A0a722fC9bffC49c921B685ECB84b94"),
        fallback_swap_contract,
        ticker,
        web3_instances: vec![Web3Instance {
            web3: web3.clone(),
            is_parity: true,
        }],
        web3,
        ctx: ctx.weak(),
        required_confirmations: 1.into(),
        chain_id: None,
        logs_block_range: DEFAULT_LOGS_BLOCK_RANGE,
    }));
    (ctx, eth_coin)
}

#[test]
/// https://github.com/ethereum/EIPs/blob/master/EIPS/eip-55.md#test-cases
fn test_check_sum_address() {
    check_sum(
        "0xfb6916095ca1df60bb79ce92ce3ea74c37c5d359",
        "0xfB6916095ca1df60bB79Ce92cE3Ea74c37c5d359",
    );
    check_sum(
        "0x52908400098527886e0f7030069857d2e4169ee7",
        "0x52908400098527886E0F7030069857D2E4169EE7",
    );
    check_sum(
        "0x8617e340b3d01fa5f11f306f4090fd50e238070d",
        "0x8617E340B3D01FA5F11F306F4090FD50E238070D",
    );
    check_sum(
        "0xde709f2102306220921060314715629080e2fb77",
        "0xde709f2102306220921060314715629080e2fb77",
    );
    check_sum(
        "0x27b1fdb04752bbc536007a920d24acb045561c26",
        "0x27b1fdb04752bbc536007a920d24acb045561c26",
    );
    check_sum(
        "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed",
        "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed",
    );
    check_sum(
        "0xfB6916095ca1df60bB79Ce92cE3Ea74c37c5d359",
        "0xfB6916095ca1df60bB79Ce92cE3Ea74c37c5d359",
    );
    check_sum(
        "0xdbF03B407c01E7cD3CBea99509d93f8DDDC8C6FB",
        "0xdbF03B407c01E7cD3CBea99509d93f8DDDC8C6FB",
    );
    check_sum(
        "0xD1220A0cf47c7B9Be7A2E6BA89F429762e7b9aDb",
        "0xD1220A0cf47c7B9Be7A2E6BA89F429762e7b9aDb",
    );
}

#[test]
fn test_is_valid_checksum_addr() {
    assert!(is_valid_checksum_addr("0xfB6916095ca1df60bB79Ce92cE3Ea74c37c5d359"));
    assert!(is_valid_checksum_addr("0x52908400098527886E0F7030069857D2E4169EE7"));
    assert!(!is_valid_checksum_addr("0x8617e340B3D01FA5F11F306F4090FD50E238070D"));
    assert!(!is_valid_checksum_addr("0xd1220A0cf47c7B9Be7A2E6BA89F429762e7b9aDb"));
}

#[test]
fn display_u256_with_point() {
    let number = U256::from_dec_str("1000000000000000000").unwrap();
    let string = display_u256_with_decimal_point(number, 18);
    assert_eq!("1.", string);

    let number = U256::from_dec_str("10000000000000000000000000000000000000000000000000000000000").unwrap();
    let string = display_u256_with_decimal_point(number, 18);
    assert_eq!("10000000000000000000000000000000000000000.", string);

    let number = U256::from_dec_str("1234567890000000000").unwrap();
    let string = display_u256_with_decimal_point(number, 18);
    assert_eq!("1.23456789", string);

    let number = U256::from_dec_str("1234567890000000000").unwrap();
    let string = display_u256_with_decimal_point(number, 16);
    assert_eq!("123.456789", string);

    let number = U256::from_dec_str("1234567890000000000").unwrap();
    let string = display_u256_with_decimal_point(number, 0);
    assert_eq!("1234567890000000000.", string);

    let number = U256::from_dec_str("1000000000000000").unwrap();
    let string = display_u256_with_decimal_point(number, 18);
    assert_eq!("0.001", string);

    let number = U256::from_dec_str("0").unwrap();
    let string = display_u256_with_decimal_point(number, 18);
    assert_eq!("0.", string);

    let number = U256::from_dec_str("0").unwrap();
    let string = display_u256_with_decimal_point(number, 0);
    assert_eq!("0.", string);
}

#[test]
fn test_wei_from_big_decimal() {
    let amount = "0.000001".parse().unwrap();
    let wei = wei_from_big_decimal(&amount, 18).unwrap();
    let expected_wei: U256 = 1000000000000u64.into();
    assert_eq!(expected_wei, wei);

    let amount = "1.000001".parse().unwrap();
    let wei = wei_from_big_decimal(&amount, 18).unwrap();
    let expected_wei: U256 = 1000001000000000000u64.into();
    assert_eq!(expected_wei, wei);

    let amount = 1.into();
    let wei = wei_from_big_decimal(&amount, 18).unwrap();
    let expected_wei: U256 = 1000000000000000000u64.into();
    assert_eq!(expected_wei, wei);

    let amount = "0.000000000000000001".parse().unwrap();
    let wei = wei_from_big_decimal(&amount, 18).unwrap();
    let expected_wei: U256 = 1u64.into();
    assert_eq!(expected_wei, wei);

    let amount = 1234.into();
    let wei = wei_from_big_decimal(&amount, 9).unwrap();
    let expected_wei: U256 = 1234000000000u64.into();
    assert_eq!(expected_wei, wei);

    let amount = 1234.into();
    let wei = wei_from_big_decimal(&amount, 0).unwrap();
    let expected_wei: U256 = 1234u64.into();
    assert_eq!(expected_wei, wei);

    let amount = 1234.into();
    let wei = wei_from_big_decimal(&amount, 1).unwrap();
    let expected_wei: U256 = 12340u64.into();
    assert_eq!(expected_wei, wei);

    let amount = "1234.12345".parse().unwrap();
    let wei = wei_from_big_decimal(&amount, 1).unwrap();
    let expected_wei: U256 = 12341u64.into();
    assert_eq!(expected_wei, wei);
}

#[test]
#[ignore]
/// temporary ignore, will refactor later to use dev chain and properly check transaction statuses
fn send_and_refund_erc20_payment() {
    let key_pair = KeyPair::from_secret_slice(
        &hex::decode("809465b17d0a4ddb3e4c69e8f23c2cabad868f51f8bed5c765ad1d6516c3306f").unwrap(),
    )
    .unwrap();
    let transport = Web3Transport::new(vec!["http://195.201.0.6:8545".into()]).unwrap();
    let web3 = Web3::new(transport);
    let ctx = MmCtxBuilder::new().into_mm_arc();
    let coin = EthCoin(Arc::new(EthCoinImpl {
        ticker: "ETH".into(),
        coin_type: EthCoinType::Erc20 {
            platform: "ETH".to_string(),
            token_addr: Address::from("0xc0eb7AeD740E1796992A08962c15661bDEB58003"),
        },
        my_address: key_pair.address(),
        key_pair,
        swap_contract_address: Address::from("0x7Bc1bBDD6A0a722fC9bffC49c921B685ECB84b94"),
        fallback_swap_contract: None,
        web3_instances: vec![Web3Instance {
            web3: web3.clone(),
            is_parity: true,
        }],
        web3,
        decimals: 18,
        gas_station_url: None,
        history_sync_state: Mutex::new(HistorySyncState::NotStarted),
        ctx: ctx.weak(),
        required_confirmations: 1.into(),
        chain_id: None,
        logs_block_range: DEFAULT_LOGS_BLOCK_RANGE,
    }));

    let payment = coin
        .send_maker_payment(
            (now_ms() / 1000) as u32 - 200,
            &DEX_FEE_ADDR_RAW_PUBKEY,
            &[1; 20],
            "0.001".parse().unwrap(),
            &coin.swap_contract_address(),
        )
        .wait()
        .unwrap();

    log!([payment]);

    block_on(Timer::sleep(60.));

    let refund = coin
        .send_maker_refunds_payment(
            &payment.tx_hex(),
            (now_ms() / 1000) as u32 - 200,
            &DEX_FEE_ADDR_RAW_PUBKEY,
            &[1; 20],
            &coin.swap_contract_address(),
        )
        .wait()
        .unwrap();

    log!([refund]);
}

#[test]
#[ignore]
/// temporary ignore, will refactor later to use dev chain and properly check transaction statuses
fn send_and_refund_eth_payment() {
    let key_pair = KeyPair::from_secret_slice(
        &hex::decode("809465b17d0a4ddb3e4c69e8f23c2cabad868f51f8bed5c765ad1d6516c3306f").unwrap(),
    )
    .unwrap();
    let transport = Web3Transport::new(vec!["http://195.201.0.6:8545".into()]).unwrap();
    let web3 = Web3::new(transport);
    let ctx = MmCtxBuilder::new().into_mm_arc();
    let coin = EthCoin(Arc::new(EthCoinImpl {
        ticker: "ETH".into(),
        coin_type: EthCoinType::Eth,
        my_address: key_pair.address(),
        key_pair,
        swap_contract_address: Address::from("0x7Bc1bBDD6A0a722fC9bffC49c921B685ECB84b94"),
        fallback_swap_contract: None,
        web3_instances: vec![Web3Instance {
            web3: web3.clone(),
            is_parity: true,
        }],
        web3,
        decimals: 18,
        gas_station_url: None,
        history_sync_state: Mutex::new(HistorySyncState::NotStarted),
        ctx: ctx.weak(),
        required_confirmations: 1.into(),
        chain_id: None,
        logs_block_range: DEFAULT_LOGS_BLOCK_RANGE,
    }));

    let payment = coin
        .send_maker_payment(
            (now_ms() / 1000) as u32 - 200,
            &DEX_FEE_ADDR_RAW_PUBKEY,
            &[1; 20],
            "0.001".parse().unwrap(),
            &coin.swap_contract_address(),
        )
        .wait()
        .unwrap();

    log!([payment]);

    block_on(Timer::sleep(60.));

    let refund = coin
        .send_maker_refunds_payment(
            &payment.tx_hex(),
            (now_ms() / 1000) as u32 - 200,
            &DEX_FEE_ADDR_RAW_PUBKEY,
            &[1; 20],
            &coin.swap_contract_address(),
        )
        .wait()
        .unwrap();

    log!([refund]);
}

#[test]
#[ignore]
fn test_nonce_several_urls() {
    let key_pair = KeyPair::from_secret_slice(
        &hex::decode("809465b17d0a4ddb3e4c69e8f23c2cabad868f51f8bed5c765ad1d6516c3306f").unwrap(),
    )
    .unwrap();
    let infura_transport = Web3Transport::new(vec![
        "https://ropsten.infura.io/v3/c01c1b4cf66642528547624e1d6d9d6b".into()
    ])
    .unwrap();
    let linkpool_transport = Web3Transport::new(vec!["https://ropsten-rpc.linkpool.io".into()]).unwrap();
    // get nonce must succeed if some nodes are down at the moment for some reason
    let failing_transport = Web3Transport::new(vec!["http://195.201.0.6:8989".into()]).unwrap();

    let web3_infura = Web3::new(infura_transport);
    let web3_linkpool = Web3::new(linkpool_transport);
    let web3_failing = Web3::new(failing_transport);

    let ctx = MmCtxBuilder::new().into_mm_arc();
    let coin = EthCoin(Arc::new(EthCoinImpl {
        ticker: "ETH".into(),
        coin_type: EthCoinType::Eth,
        my_address: key_pair.address(),
        key_pair,
        swap_contract_address: Address::from("0x7Bc1bBDD6A0a722fC9bffC49c921B685ECB84b94"),
        fallback_swap_contract: None,
        web3_instances: vec![
            Web3Instance {
                web3: web3_infura.clone(),
                is_parity: false,
            },
            Web3Instance {
                web3: web3_linkpool,
                is_parity: false,
            },
            Web3Instance {
                web3: web3_failing,
                is_parity: false,
            },
        ],
        web3: web3_infura,
        decimals: 18,
        gas_station_url: Some("https://ethgasstation.info/json/ethgasAPI.json".into()),
        history_sync_state: Mutex::new(HistorySyncState::NotStarted),
        ctx: ctx.weak(),
        required_confirmations: 1.into(),
        chain_id: None,
        logs_block_range: DEFAULT_LOGS_BLOCK_RANGE,
    }));

    log!("My address "[coin.my_address]);
    log!("before payment");
    let payment = coin.send_to_address(coin.my_address, 200000000.into()).wait().unwrap();

    log!([payment]);
    let new_nonce = get_addr_nonce(coin.my_address, coin.web3_instances.clone())
        .wait()
        .unwrap();
    log!([new_nonce]);
}

#[test]
fn test_wait_for_payment_spend_timeout() {
    EthCoinImpl::spend_events.mock_safe(|_, _, _, _| MockResult::Return(Box::new(futures01::future::ok(vec![]))));
    EthCoin::current_block.mock_safe(|_| MockResult::Return(Box::new(futures01::future::ok(900))));

    let key_pair = KeyPair::from_secret_slice(
        &hex::decode("809465b17d0a4ddb3e4c69e8f23c2cabad868f51f8bed5c765ad1d6516c3306f").unwrap(),
    )
    .unwrap();
    let transport = Web3Transport::new(vec!["http://195.201.0.6:8555".into()]).unwrap();
    let web3 = Web3::new(transport);
    let ctx = MmCtxBuilder::new().into_mm_arc();

    let coin = EthCoinImpl {
        coin_type: EthCoinType::Eth,
        decimals: 18,
        gas_station_url: None,
        history_sync_state: Mutex::new(HistorySyncState::NotEnabled),
        my_address: key_pair.address(),
        key_pair,
        swap_contract_address: Address::from("0x7Bc1bBDD6A0a722fC9bffC49c921B685ECB84b94"),
        fallback_swap_contract: None,
        ticker: "ETH".into(),
        web3_instances: vec![Web3Instance {
            web3: web3.clone(),
            is_parity: true,
        }],
        web3,
        ctx: ctx.weak(),
        required_confirmations: 1.into(),
        chain_id: None,
        logs_block_range: DEFAULT_LOGS_BLOCK_RANGE,
    };

    let coin = EthCoin(Arc::new(coin));
    let wait_until = (now_ms() / 1000) - 1;
    let from_block = 1;
    // raw transaction bytes of https://etherscan.io/tx/0x0869be3e5d4456a29d488a533ad6c118620fef450f36778aecf31d356ff8b41f
    let tx_bytes = [
        248, 240, 3, 133, 1, 42, 5, 242, 0, 131, 2, 73, 240, 148, 133, 0, 175, 192, 188, 82, 20, 114, 128, 130, 22, 51,
        38, 194, 255, 12, 115, 244, 168, 113, 135, 110, 205, 245, 24, 127, 34, 254, 184, 132, 21, 44, 243, 175, 73, 33,
        143, 82, 117, 16, 110, 27, 133, 82, 200, 114, 233, 42, 140, 198, 35, 21, 201, 249, 187, 180, 20, 46, 148, 40,
        9, 228, 193, 130, 71, 199, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 152, 41, 132, 9, 201, 73, 19, 94, 237, 137, 35,
        61, 4, 194, 207, 239, 152, 75, 175, 245, 157, 174, 10, 214, 161, 207, 67, 70, 87, 246, 231, 212, 47, 216, 119,
        68, 237, 197, 125, 141, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 93, 72, 125, 102, 28, 159, 180, 237, 198, 97, 87, 80, 82, 200, 104, 40, 245,
        221, 7, 28, 122, 104, 91, 99, 1, 159, 140, 25, 131, 101, 74, 87, 50, 168, 146, 187, 90, 160, 51, 1, 123, 247,
        6, 108, 165, 181, 188, 40, 56, 47, 211, 229, 221, 73, 5, 15, 89, 81, 117, 225, 216, 108, 98, 226, 119, 232, 94,
        184, 42, 106,
    ];

    assert!(coin
        .wait_for_tx_spend(&tx_bytes, wait_until, from_block, &coin.swap_contract_address())
        .wait()
        .is_err());
}

#[test]
fn test_search_for_swap_tx_spend_was_spent() {
    let key_pair = KeyPair::from_secret_slice(
        &hex::decode("809465b17d0a4ddb3e4c69e8f23c2cabad868f51f8bed5c765ad1d6516c3306f").unwrap(),
    )
    .unwrap();
    let transport = Web3Transport::new(vec![
        "https://ropsten.infura.io/v3/c01c1b4cf66642528547624e1d6d9d6b".into()
    ])
    .unwrap();
    let web3 = Web3::new(transport);
    let ctx = MmCtxBuilder::new().into_mm_arc();

    let swap_contract_address = Address::from("0x7Bc1bBDD6A0a722fC9bffC49c921B685ECB84b94");
    let coin = EthCoin(Arc::new(EthCoinImpl {
        coin_type: EthCoinType::Eth,
        decimals: 18,
        gas_station_url: None,
        history_sync_state: Mutex::new(HistorySyncState::NotEnabled),
        my_address: key_pair.address(),
        key_pair,
        swap_contract_address,
        fallback_swap_contract: None,
        ticker: "ETH".into(),
        web3_instances: vec![Web3Instance {
            web3: web3.clone(),
            is_parity: true,
        }],
        web3,
        ctx: ctx.weak(),
        required_confirmations: 1.into(),
        chain_id: None,
        logs_block_range: DEFAULT_LOGS_BLOCK_RANGE,
    }));

    // raw transaction bytes of https://ropsten.etherscan.io/tx/0xb1c987e2ac79581bb8718267b5cb49a18274890494299239d1d0dfdb58d6d76a
    let payment_tx = [
        248, 240, 52, 132, 119, 53, 148, 0, 131, 2, 73, 240, 148, 123, 193, 187, 221, 106, 10, 114, 47, 201, 191, 252,
        73, 201, 33, 182, 133, 236, 184, 75, 148, 135, 71, 13, 228, 223, 130, 0, 0, 184, 132, 21, 44, 243, 175, 188,
        96, 248, 252, 165, 132, 81, 30, 243, 34, 85, 165, 46, 224, 176, 90, 137, 30, 19, 123, 224, 67, 83, 53, 74, 57,
        148, 140, 95, 45, 70, 147, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 117, 244, 28, 175, 51, 95, 91, 184, 141, 201,
        45, 116, 26, 102, 210, 119, 151, 124, 143, 52, 215, 128, 89, 116, 30, 25, 35, 128, 122, 186, 177, 228, 149,
        250, 55, 53, 62, 196, 51, 72, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 93, 56, 62, 80, 28, 160, 65, 22, 195, 212, 184, 202, 226, 151, 224, 111,
        174, 31, 160, 219, 39, 69, 137, 37, 8, 127, 177, 4, 104, 248, 27, 41, 245, 176, 131, 188, 215, 136, 160, 91,
        134, 199, 67, 1, 58, 57, 103, 23, 215, 176, 64, 124, 1, 44, 88, 161, 200, 160, 64, 110, 13, 145, 127, 180, 27,
        171, 131, 253, 90, 48, 147,
    ];
    // raw transaction bytes of https://ropsten.etherscan.io/tx/0xcb7c14d3ff309996d582400369393b6fa42314c52245115d4a3f77f072c36da9
    let spend_tx = [
        249, 1, 9, 37, 132, 119, 53, 148, 0, 131, 2, 73, 240, 148, 123, 193, 187, 221, 106, 10, 114, 47, 201, 191, 252,
        73, 201, 33, 182, 133, 236, 184, 75, 148, 128, 184, 164, 2, 237, 41, 43, 188, 96, 248, 252, 165, 132, 81, 30,
        243, 34, 85, 165, 46, 224, 176, 90, 137, 30, 19, 123, 224, 67, 83, 53, 74, 57, 148, 140, 95, 45, 70, 147, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 71, 13, 228, 223, 130, 0, 0, 168, 151, 11,
        232, 224, 253, 63, 180, 26, 114, 23, 184, 27, 10, 161, 80, 178, 251, 73, 204, 80, 174, 97, 118, 149, 204, 186,
        187, 243, 185, 19, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 157, 73, 251, 238, 138, 245, 142, 240, 85, 44, 209, 63, 194, 242,
        109, 242, 246, 6, 76, 176, 27, 160, 29, 157, 226, 23, 81, 174, 34, 82, 93, 182, 41, 248, 119, 42, 221, 214, 38,
        243, 128, 2, 235, 208, 193, 192, 74, 208, 242, 26, 221, 83, 54, 74, 160, 111, 29, 92, 8, 75, 61, 97, 103, 199,
        100, 189, 72, 74, 221, 144, 66, 170, 68, 121, 29, 105, 19, 194, 35, 245, 196, 131, 236, 29, 105, 101, 30,
    ];
    let spend_tx = FoundSwapTxSpend::Spent(signed_eth_tx_from_bytes(&spend_tx).unwrap().into());

    let found_tx = coin
        .search_for_swap_tx_spend(&payment_tx, swap_contract_address, 6051857)
        .unwrap()
        .unwrap();
    assert_eq!(spend_tx, found_tx);
}

#[test]
fn test_search_for_swap_tx_spend_was_refunded() {
    let key_pair = KeyPair::from_secret_slice(
        &hex::decode("809465b17d0a4ddb3e4c69e8f23c2cabad868f51f8bed5c765ad1d6516c3306f").unwrap(),
    )
    .unwrap();
    let transport = Web3Transport::new(vec![
        "https://ropsten.infura.io/v3/c01c1b4cf66642528547624e1d6d9d6b".into()
    ])
    .unwrap();
    let web3 = Web3::new(transport);
    let ctx = MmCtxBuilder::new().into_mm_arc();

    let swap_contract_address = Address::from("0x7Bc1bBDD6A0a722fC9bffC49c921B685ECB84b94");
    let coin = EthCoin(Arc::new(EthCoinImpl {
        coin_type: EthCoinType::Erc20 {
            platform: "ETH".to_string(),
            token_addr: Address::from("0xc0eb7aed740e1796992a08962c15661bdeb58003"),
        },
        decimals: 18,
        gas_station_url: None,
        history_sync_state: Mutex::new(HistorySyncState::NotEnabled),
        my_address: key_pair.address(),
        key_pair,
        swap_contract_address,
        fallback_swap_contract: None,
        ticker: "ETH".into(),
        web3_instances: vec![Web3Instance {
            web3: web3.clone(),
            is_parity: true,
        }],
        web3,
        ctx: ctx.weak(),
        required_confirmations: 1.into(),
        chain_id: None,
        logs_block_range: DEFAULT_LOGS_BLOCK_RANGE,
    }));

    // raw transaction bytes of https://ropsten.etherscan.io/tx/0xe18bbca69dea9a4624e1f5b0b2021d5fe4c8daa03f36084a8ba011b08e5cd938
    let payment_tx = [
        249, 1, 43, 130, 10, 96, 132, 149, 2, 249, 0, 131, 2, 73, 240, 148, 123, 193, 187, 221, 106, 10, 114, 47, 201,
        191, 252, 73, 201, 33, 182, 133, 236, 184, 75, 148, 128, 184, 196, 155, 65, 91, 42, 192, 158, 192, 175, 210,
        198, 159, 244, 116, 46, 255, 28, 236, 147, 240, 68, 91, 16, 19, 6, 59, 187, 149, 138, 179, 151, 121, 47, 14,
        80, 251, 147, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 141, 126, 164, 198,
        128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 192, 235, 122, 237, 116, 14, 23, 150, 153, 42, 8, 150, 44, 21, 102,
        27, 222, 181, 128, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 216, 153, 121, 65, 221, 19, 70, 233, 35, 17, 24, 213,
        104, 93, 134, 98, 148, 245, 158, 91, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 93,
        23, 98, 207, 27, 160, 4, 198, 61, 242, 141, 248, 157, 72, 229, 2, 162, 163, 250, 159, 26, 66, 37, 42, 159, 35,
        58, 94, 57, 121, 252, 166, 34, 25, 206, 193, 113, 198, 160, 68, 125, 142, 153, 210, 177, 60, 173, 67, 127, 138,
        52, 112, 9, 49, 108, 109, 44, 177, 142, 9, 124, 10, 200, 37, 100, 52, 137, 196, 74, 67, 192,
    ];
    // raw transaction bytes of https://ropsten.etherscan.io/tx/0x9a50ac4d1737f4f04b94177996da7fa942b09469de52cfdadce891cd85afc37c
    let refund_tx = [
        249, 1, 11, 130, 10, 97, 132, 149, 2, 249, 0, 131, 2, 73, 240, 148, 123, 193, 187, 221, 106, 10, 114, 47, 201,
        191, 252, 73, 201, 33, 182, 133, 236, 184, 75, 148, 128, 184, 164, 70, 252, 2, 148, 192, 158, 192, 175, 210,
        198, 159, 244, 116, 46, 255, 28, 236, 147, 240, 68, 91, 16, 19, 6, 59, 187, 149, 138, 179, 151, 121, 47, 14,
        80, 251, 147, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 141, 126, 164, 198,
        128, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 192, 235, 122, 237, 116, 14, 23, 150, 153, 42, 8, 150, 44, 21, 102, 27, 222, 181,
        128, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 216, 153, 121, 65, 221, 19, 70, 233, 35, 17, 24, 213, 104, 93, 134,
        98, 148, 245, 158, 91, 28, 160, 127, 220, 190, 77, 221, 188, 140, 162, 198, 6, 127, 102, 222, 66, 38, 96, 10,
        19, 27, 208, 119, 219, 60, 231, 2, 118, 91, 169, 99, 78, 209, 135, 160, 51, 115, 90, 189, 124, 172, 205, 134,
        203, 159, 238, 40, 39, 99, 88, 48, 160, 189, 37, 60, 20, 117, 65, 238, 36, 98, 226, 48, 22, 235, 86, 183,
    ];
    let refund_tx = FoundSwapTxSpend::Refunded(signed_eth_tx_from_bytes(&refund_tx).unwrap().into());

    let found_tx = coin
        .search_for_swap_tx_spend(&payment_tx, swap_contract_address, 5886908)
        .unwrap()
        .unwrap();
    assert_eq!(refund_tx, found_tx);
}

#[test]
fn test_withdraw_impl_manual_fee() {
    let (ctx, coin) = eth_coin_for_test(EthCoinType::Eth, vec!["http://dummy.dummy".into()], None);

    EthCoin::my_balance.mock_safe(|_| {
        let balance = wei_from_big_decimal(&1000000000.into(), 18).unwrap();
        MockResult::Return(Box::new(futures01::future::ok(balance)))
    });
    get_addr_nonce.mock_safe(|_, _| MockResult::Return(Box::new(futures01::future::ok(0.into()))));

    let withdraw_req = WithdrawRequest {
        amount: 1.into(),
        to: "0x7Bc1bBDD6A0a722fC9bffC49c921B685ECB84b94".to_string(),
        coin: "ETH".to_string(),
        max: false,
        fee: Some(WithdrawFee::EthGas {
            gas: 150000,
            gas_price: 1.into(),
        }),
    };
    coin.my_balance().wait().unwrap();

    let tx_details = block_on(withdraw_impl(ctx, coin.clone(), withdraw_req)).unwrap();
    let expected = Some(
        EthTxFeeDetails {
            coin: "ETH".into(),
            gas_price: "0.000000001".parse().unwrap(),
            gas: 150000,
            total_fee: "0.00015".parse().unwrap(),
        }
        .into(),
    );
    assert_eq!(expected, tx_details.fee_details);
}

#[test]
fn test_withdraw_impl_fee_details() {
    let (ctx, coin) = eth_coin_for_test(
        EthCoinType::Erc20 {
            platform: "ETH".to_string(),
            token_addr: Address::from("0x2b294F029Fde858b2c62184e8390591755521d8E"),
        },
        vec!["http://dummy.dummy".into()],
        None,
    );

    EthCoin::my_balance.mock_safe(|_| {
        let balance = wei_from_big_decimal(&1000000000.into(), 18).unwrap();
        MockResult::Return(Box::new(futures01::future::ok(balance)))
    });
    get_addr_nonce.mock_safe(|_, _| MockResult::Return(Box::new(futures01::future::ok(0.into()))));

    let withdraw_req = WithdrawRequest {
        amount: 1.into(),
        to: "0x7Bc1bBDD6A0a722fC9bffC49c921B685ECB84b94".to_string(),
        coin: "JST".to_string(),
        max: false,
        fee: Some(WithdrawFee::EthGas {
            gas: 150000,
            gas_price: 1.into(),
        }),
    };
    coin.my_balance().wait().unwrap();

    let tx_details = block_on(withdraw_impl(ctx, coin.clone(), withdraw_req)).unwrap();
    let expected = Some(
        EthTxFeeDetails {
            coin: "ETH".into(),
            gas_price: "0.000000001".parse().unwrap(),
            gas: 150000,
            total_fee: "0.00015".parse().unwrap(),
        }
        .into(),
    );
    assert_eq!(expected, tx_details.fee_details);
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn test_nonce_lock() {
    use common::for_tests::wait_for_log;
    use futures::future::join_all;

    // send several transactions concurrently to check that they are not using same nonce
    // using real ETH dev node
    let (ctx, coin) = eth_coin_for_test(EthCoinType::Eth, vec!["http://195.201.0.6:8565".into()], None);
    let mut futures = vec![];
    for _ in 0..5 {
        futures.push(sign_and_send_transaction_impl(
            ctx.clone(),
            coin.clone(),
            1000000000000u64.into(),
            Action::Call(coin.my_address),
            vec![],
            21000.into(),
        ));
    }
    let results = block_on(join_all(futures));
    for result in results {
        result.unwrap();
    }
    // Waiting for NONCE_LOCK… might not appear at all if waiting takes less than 0.5 seconds
    // but all transactions are sent successfully still
    // wait_for_log(&ctx.log, 1.1, &|line| line.contains("Waiting for NONCE_LOCK…")));
    block_on(wait_for_log(&ctx, 1.1, |line| line.contains("get_addr_nonce…"))).unwrap();
}

#[test]
fn test_add_ten_pct_one_gwei() {
    let num = wei_from_big_decimal(&"0.1".parse().unwrap(), 9).unwrap();
    let expected = wei_from_big_decimal(&"1.1".parse().unwrap(), 9).unwrap();
    let actual = increase_by_percent_one_gwei(num, GAS_PRICE_PERCENT);
    assert_eq!(expected, actual);

    let num = wei_from_big_decimal(&"9.9".parse().unwrap(), 9).unwrap();
    let expected = wei_from_big_decimal(&"10.9".parse().unwrap(), 9).unwrap();
    let actual = increase_by_percent_one_gwei(num, GAS_PRICE_PERCENT);
    assert_eq!(expected, actual);

    let num = wei_from_big_decimal(&"30.1".parse().unwrap(), 9).unwrap();
    let expected = wei_from_big_decimal(&"33.11".parse().unwrap(), 9).unwrap();
    let actual = increase_by_percent_one_gwei(num, GAS_PRICE_PERCENT);
    assert_eq!(expected, actual);
}

#[test]
fn get_sender_trade_preimage() {
    /// Trade fee for the ETH coin is `2 * 150_000 * gas_price` always.
    fn expected_fee(gas_price: u64) -> TradeFee {
        let amount = u256_to_big_decimal((2 * 150_000 * gas_price).into(), 18).expect("!u256_to_big_decimal");
        TradeFee {
            coin: "ETH".to_owned(),
            amount: amount.into(),
            paid_from_trading_vol: false,
        }
    }

    EthCoinImpl::get_gas_price.mock_safe(|_| MockResult::Return(Box::new(futures01::future::ok(GAS_PRICE.into()))));

    let (_ctx, coin) = eth_coin_for_test(EthCoinType::Eth, vec!["http://dummy.dummy".into()], None);

    let actual = coin
        .get_sender_trade_fee(
            TradePreimageValue::UpperBound(150.into()),
            FeeApproxStage::WithoutApprox,
        )
        .wait()
        .expect("!get_sender_trade_fee");
    let expected = expected_fee(GAS_PRICE);
    assert_eq!(actual, expected);

    let value = u256_to_big_decimal(100.into(), 18).expect("!u256_to_big_decimal");
    let actual = coin
        .get_sender_trade_fee(TradePreimageValue::Exact(value), FeeApproxStage::OrderIssue)
        .wait()
        .expect("!get_sender_trade_fee");
    let expected = expected_fee(GAS_PRICE_APPROXIMATION_ON_ORDER_ISSUE);
    assert_eq!(actual, expected);

    let value = u256_to_big_decimal(1.into(), 18).expect("!u256_to_big_decimal");
    let actual = coin
        .get_sender_trade_fee(TradePreimageValue::Exact(value), FeeApproxStage::StartSwap)
        .wait()
        .expect("!get_sender_trade_fee");
    let expected = expected_fee(GAS_PRICE_APPROXIMATION_ON_START_SWAP);
    assert_eq!(actual, expected);

    let value = u256_to_big_decimal(10000000000u64.into(), 18).expect("!u256_to_big_decimal");
    let actual = coin
        .get_sender_trade_fee(TradePreimageValue::Exact(value), FeeApproxStage::TradePreimage)
        .wait()
        .expect("!get_sender_trade_fee");
    let expected = expected_fee(GAS_PRICE_APPROXIMATION_ON_TRADE_PREIMAGE);
    assert_eq!(actual, expected);
}

#[test]
fn get_erc20_sender_trade_preimage() {
    static mut ALLOWANCE: u64 = 0;
    EthCoin::allowance
        .mock_safe(|_, _| MockResult::Return(Box::new(futures01::future::ok(unsafe { ALLOWANCE.into() }))));

    EthCoinImpl::get_gas_price.mock_safe(|_| MockResult::Return(Box::new(futures01::future::ok(GAS_PRICE.into()))));

    fn expected_trade_fee(gas_limit: u64, gas_price: u64) -> TradeFee {
        let amount = u256_to_big_decimal((gas_limit * gas_price).into(), 18).expect("!u256_to_big_decimal");
        TradeFee {
            coin: "ETH".to_owned(),
            amount: amount.into(),
            paid_from_trading_vol: false,
        }
    }

    let (_ctx, coin) = eth_coin_for_test(
        EthCoinType::Erc20 {
            platform: "ETH".to_string(),
            token_addr: Address::default(),
        },
        vec!["http://dummy.dummy".into()],
        None,
    );

    // value is allowed
    unsafe { ALLOWANCE = 1000 };
    let value = u256_to_big_decimal(1000.into(), 18).expect("u256_to_big_decimal");
    let actual = coin
        .get_sender_trade_fee(TradePreimageValue::UpperBound(value), FeeApproxStage::WithoutApprox)
        .wait()
        .expect("!get_sender_trade_fee");
    log!([actual.amount.to_decimal()]);
    assert_eq!(actual, expected_trade_fee(300_000, GAS_PRICE));

    // value is greater than allowance
    unsafe { ALLOWANCE = 999 };
    let value = u256_to_big_decimal(1000.into(), 18).expect("u256_to_big_decimal");
    let actual = coin
        .get_sender_trade_fee(TradePreimageValue::UpperBound(value), FeeApproxStage::StartSwap)
        .wait()
        .expect("!get_sender_trade_fee");
    assert_eq!(
        actual,
        expected_trade_fee(350_000, GAS_PRICE_APPROXIMATION_ON_START_SWAP)
    );

    // value is allowed
    unsafe { ALLOWANCE = 1000 };
    let value = u256_to_big_decimal(999.into(), 18).expect("u256_to_big_decimal");
    let actual = coin
        .get_sender_trade_fee(TradePreimageValue::Exact(value), FeeApproxStage::OrderIssue)
        .wait()
        .expect("!get_sender_trade_fee");
    assert_eq!(
        actual,
        expected_trade_fee(300_000, GAS_PRICE_APPROXIMATION_ON_ORDER_ISSUE)
    );

    // value is greater than allowance
    unsafe { ALLOWANCE = 1000 };
    let value = u256_to_big_decimal(1500.into(), 18).expect("u256_to_big_decimal");
    let actual = coin
        .get_sender_trade_fee(TradePreimageValue::Exact(value), FeeApproxStage::TradePreimage)
        .wait()
        .expect("!get_sender_trade_fee");
    assert_eq!(
        actual,
        expected_trade_fee(350_000, GAS_PRICE_APPROXIMATION_ON_TRADE_PREIMAGE)
    );
}

#[test]
fn get_receiver_trade_preimage() {
    EthCoinImpl::get_gas_price.mock_safe(|_| MockResult::Return(Box::new(futures01::future::ok(GAS_PRICE.into()))));

    let (_ctx, coin) = eth_coin_for_test(EthCoinType::Eth, vec!["http://dummy.dummy".into()], None);
    let amount = u256_to_big_decimal((150_000 * GAS_PRICE).into(), 18).expect("!u256_to_big_decimal");
    let expected_fee = TradeFee {
        coin: "ETH".to_owned(),
        amount: amount.into(),
        paid_from_trading_vol: false,
    };

    let actual = coin
        .get_receiver_trade_fee(FeeApproxStage::WithoutApprox)
        .wait()
        .expect("!get_sender_trade_fee");
    assert_eq!(actual, expected_fee);
}

#[test]
fn test_get_fee_to_send_taker_fee() {
    const DEX_FEE_AMOUNT: u64 = 100_000;
    const TRANSFER_GAS_LIMIT: u64 = 40_000;

    EthCoinImpl::get_gas_price.mock_safe(|_| MockResult::Return(Box::new(futures01::future::ok(GAS_PRICE.into()))));
    EthCoinImpl::estimate_gas
        .mock_safe(|_, _| MockResult::Return(Box::new(futures01::future::ok(TRANSFER_GAS_LIMIT.into()))));

    // fee to send taker fee is `TRANSFER_GAS_LIMIT * gas_price` always.
    let amount = u256_to_big_decimal((TRANSFER_GAS_LIMIT * GAS_PRICE).into(), 18).expect("!u256_to_big_decimal");
    let expected_fee = TradeFee {
        coin: "ETH".to_owned(),
        amount: amount.into(),
        paid_from_trading_vol: false,
    };

    let dex_fee_amount = u256_to_big_decimal(DEX_FEE_AMOUNT.into(), 18).expect("!u256_to_big_decimal");

    let (_ctx, coin) = eth_coin_for_test(EthCoinType::Eth, vec!["http://dummy.dummy".into()], None);
    let actual = coin
        .get_fee_to_send_taker_fee(dex_fee_amount.clone(), FeeApproxStage::WithoutApprox)
        .wait()
        .expect("!get_fee_to_send_taker_fee");
    assert_eq!(actual, expected_fee);

    let (_ctx, coin) = eth_coin_for_test(
        EthCoinType::Erc20 {
            platform: "ETH".to_string(),
            token_addr: Address::from("0xaD22f63404f7305e4713CcBd4F296f34770513f4"),
        },
        vec!["http://dummy.dummy".into()],
        None,
    );
    let actual = coin
        .get_fee_to_send_taker_fee(dex_fee_amount.clone(), FeeApproxStage::WithoutApprox)
        .wait()
        .expect("!get_fee_to_send_taker_fee");
    assert_eq!(actual, expected_fee);
}

/// Some ERC20 tokens return the `error: -32016, message: \"The execution failed due to an exception.\"` error
/// if the balance is insufficient.
/// So [`EthCoin::get_fee_to_send_taker_fee`] must return [`TradePreimageError::NotSufficientBalance`].
///
/// Please note this test doesn't work correctly now,
/// because as of now [`EthCoin::get_fee_to_send_taker_fee`] doesn't process the `Exception` web3 error correctly.
#[test]
#[ignore]
fn test_get_fee_to_send_taker_fee_insufficient_balance() {
    const DEX_FEE_AMOUNT: u64 = 100_000_000_000;

    EthCoinImpl::get_gas_price.mock_safe(|_| MockResult::Return(Box::new(futures01::future::ok(40.into()))));
    let (_ctx, coin) = eth_coin_for_test(
        EthCoinType::Erc20 {
            platform: "ETH".to_string(),
            token_addr: Address::from("0xaD22f63404f7305e4713CcBd4F296f34770513f4"),
        },
        vec!["http://eth1.cipig.net:8555".into()],
        None,
    );
    let dex_fee_amount = u256_to_big_decimal(DEX_FEE_AMOUNT.into(), 18).expect("!u256_to_big_decimal");

    let error = coin
        .get_fee_to_send_taker_fee(dex_fee_amount.clone(), FeeApproxStage::WithoutApprox)
        .wait()
        .unwrap_err();
    log!((error));
    assert!(
        matches!(error.get_inner(), TradePreimageError::NotSufficientBalance { .. }),
        "Expected TradePreimageError::NotSufficientBalance"
    );
}

#[test]
fn validate_dex_fee_invalid_sender_eth() {
    let (_ctx, coin) = eth_coin_for_test(EthCoinType::Eth, vec!["http://eth1.cipig.net:8555".into()], None);
    // the real dex fee sent on mainnet
    // https://etherscan.io/tx/0x7e9ca16c85efd04ee5e31f2c1914b48f5606d6f9ce96ecce8c96d47d6857278f
    let tx = coin
        .web3
        .eth()
        .transaction(TransactionId::Hash(
            "0x7e9ca16c85efd04ee5e31f2c1914b48f5606d6f9ce96ecce8c96d47d6857278f".into(),
        ))
        .wait()
        .unwrap()
        .unwrap();
    let tx = signed_tx_from_web3_tx(tx).unwrap().into();
    let amount: BigDecimal = "0.000526435076465".parse().unwrap();
    let validate_err = coin
        .validate_fee(&tx, &*DEX_FEE_ADDR_RAW_PUBKEY, &*DEX_FEE_ADDR_RAW_PUBKEY, &amount, 0)
        .wait()
        .unwrap_err();
    assert!(validate_err.contains("was sent from wrong address"));
}

#[test]
fn validate_dex_fee_invalid_sender_erc() {
    let (_ctx, coin) = eth_coin_for_test(
        EthCoinType::Erc20 {
            platform: "ETH".to_string(),
            token_addr: "0xa1d6df714f91debf4e0802a542e13067f31b8262".into(),
        },
        vec!["http://eth1.cipig.net:8555".into()],
        None,
    );
    // the real dex fee sent on mainnet
    // https://etherscan.io/tx/0xd6403b41c79f9c9e9c83c03d920ee1735e7854d85d94cef48d95dfeca95cd600
    let tx = coin
        .web3
        .eth()
        .transaction(TransactionId::Hash(
            "0xd6403b41c79f9c9e9c83c03d920ee1735e7854d85d94cef48d95dfeca95cd600".into(),
        ))
        .wait()
        .unwrap()
        .unwrap();
    let tx = signed_tx_from_web3_tx(tx).unwrap().into();
    let amount: BigDecimal = "5.548262548262548262".parse().unwrap();
    let validate_err = coin
        .validate_fee(&tx, &*DEX_FEE_ADDR_RAW_PUBKEY, &*DEX_FEE_ADDR_RAW_PUBKEY, &amount, 0)
        .wait()
        .unwrap_err();
    assert!(validate_err.contains("was sent from wrong address"));
}

fn sender_compressed_pub(tx: &SignedEthTx) -> [u8; 33] {
    let raw_pubkey = tx.public.unwrap();
    let secp_public = PublicKey::parse_slice(&raw_pubkey, None).unwrap();
    secp_public.serialize_compressed()
}

#[test]
fn validate_dex_fee_eth_confirmed_before_min_block() {
    let (_ctx, coin) = eth_coin_for_test(EthCoinType::Eth, vec!["http://eth1.cipig.net:8555".into()], None);
    // the real dex fee sent on mainnet
    // https://etherscan.io/tx/0x7e9ca16c85efd04ee5e31f2c1914b48f5606d6f9ce96ecce8c96d47d6857278f
    let tx = coin
        .web3
        .eth()
        .transaction(TransactionId::Hash(
            "0x7e9ca16c85efd04ee5e31f2c1914b48f5606d6f9ce96ecce8c96d47d6857278f".into(),
        ))
        .wait()
        .unwrap()
        .unwrap();
    let tx = signed_tx_from_web3_tx(tx).unwrap();
    let compressed_public = sender_compressed_pub(&tx);
    let tx = tx.into();
    let amount: BigDecimal = "0.000526435076465".parse().unwrap();
    let validate_err = coin
        .validate_fee(&tx, &compressed_public, &*DEX_FEE_ADDR_RAW_PUBKEY, &amount, 11784793)
        .wait()
        .unwrap_err();
    assert!(validate_err.contains("confirmed before min_block"));
}

#[test]
fn validate_dex_fee_erc_confirmed_before_min_block() {
    let (_ctx, coin) = eth_coin_for_test(
        EthCoinType::Erc20 {
            platform: "ETH".to_string(),
            token_addr: "0xa1d6df714f91debf4e0802a542e13067f31b8262".into(),
        },
        vec!["http://eth1.cipig.net:8555".into()],
        None,
    );
    // the real dex fee sent on mainnet
    // https://etherscan.io/tx/0xd6403b41c79f9c9e9c83c03d920ee1735e7854d85d94cef48d95dfeca95cd600
    let tx = coin
        .web3
        .eth()
        .transaction(TransactionId::Hash(
            "0xd6403b41c79f9c9e9c83c03d920ee1735e7854d85d94cef48d95dfeca95cd600".into(),
        ))
        .wait()
        .unwrap()
        .unwrap();

    let tx = signed_tx_from_web3_tx(tx).unwrap();
    let compressed_public = sender_compressed_pub(&tx);
    let tx = tx.into();
    let amount: BigDecimal = "5.548262548262548262".parse().unwrap();
    let validate_err = coin
        .validate_fee(&tx, &compressed_public, &*DEX_FEE_ADDR_RAW_PUBKEY, &amount, 11823975)
        .wait()
        .unwrap_err();
    assert!(validate_err.contains("confirmed before min_block"));
}

#[test]
fn test_negotiate_swap_contract_addr_no_fallback() {
    let (_, coin) = eth_coin_for_test(EthCoinType::Eth, vec!["http://eth1.cipig.net:8555".into()], None);

    let input = None;
    let error = coin.negotiate_swap_contract_addr(input).unwrap_err().into_inner();
    assert_eq!(NegotiateSwapContractAddrErr::NoOtherAddrAndNoFallback, error);

    let slice: &[u8] = &[1; 1];
    let error = coin.negotiate_swap_contract_addr(Some(slice)).unwrap_err().into_inner();
    assert_eq!(
        NegotiateSwapContractAddrErr::InvalidOtherAddrLen(slice.to_vec().into()),
        error
    );

    let slice: &[u8] = &[1; 20];
    let error = coin.negotiate_swap_contract_addr(Some(slice)).unwrap_err().into_inner();
    assert_eq!(
        NegotiateSwapContractAddrErr::UnexpectedOtherAddr(slice.to_vec().into()),
        error
    );

    let slice: &[u8] = coin.swap_contract_address.as_ref();
    let result = coin.negotiate_swap_contract_addr(Some(slice)).unwrap();
    assert_eq!(Some(slice.to_vec().into()), result);
}

#[test]
fn test_negotiate_swap_contract_addr_has_fallback() {
    let fallback = "0x8500AFc0bc5214728082163326C2FF0C73f4a871".into();

    let (_, coin) = eth_coin_for_test(
        EthCoinType::Eth,
        vec!["http://eth1.cipig.net:8555".into()],
        Some(fallback),
    );

    let input = None;
    let result = coin.negotiate_swap_contract_addr(input).unwrap();
    assert_eq!(Some(fallback.to_vec().into()), result);

    let slice: &[u8] = &[1; 1];
    let error = coin.negotiate_swap_contract_addr(Some(slice)).unwrap_err().into_inner();
    assert_eq!(
        NegotiateSwapContractAddrErr::InvalidOtherAddrLen(slice.to_vec().into()),
        error
    );

    let slice: &[u8] = &[1; 20];
    let error = coin.negotiate_swap_contract_addr(Some(slice)).unwrap_err().into_inner();
    assert_eq!(
        NegotiateSwapContractAddrErr::UnexpectedOtherAddr(slice.to_vec().into()),
        error
    );

    let slice: &[u8] = coin.swap_contract_address.as_ref();
    let result = coin.negotiate_swap_contract_addr(Some(slice)).unwrap();
    assert_eq!(Some(slice.to_vec().into()), result);

    let slice: &[u8] = fallback.as_ref();
    let result = coin.negotiate_swap_contract_addr(Some(slice)).unwrap();
    assert_eq!(Some(fallback.to_vec().into()), result);
}
