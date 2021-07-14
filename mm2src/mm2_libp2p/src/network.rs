use crate::RelayAddress;
use libp2p::PeerId;

pub const NETID_7777: u16 = 7777;

#[cfg_attr(target_arch = "wasm32", allow(dead_code))]
const ALL_NETID_7777_SEEDNODES: &[(&str, &str)] = &[
    (
        "12D3KooWEsuiKcQaBaKEzuMtT6uFjs89P1E8MK3wGRZbeuCbCw6P",
        "168.119.236.241",
    ),
    (
        "12D3KooWKxavLCJVrQ5Gk1kd9m6cohctGQBmiKPS9XQFoXEoyGmS",
        "168.119.236.249",
    ),
    (
        "12D3KooWAToxtunEBWCoAHjefSv74Nsmxranw8juy3eKEdrQyGRF",
        "168.119.236.240",
    ),
    (
        "12D3KooWSmEi8ypaVzFA1AGde2RjxNW5Pvxw3qa2fVe48PjNs63R",
        "168.119.236.239",
    ),
    (
        "12D3KooWHKkHiNhZtKceQehHhPqwU5W1jXpoVBgS1qst899GjvTm",
        "168.119.236.251",
    ),
    ("12D3KooWMrjLmrv8hNgAoVf1RfumfjyPStzd4nv5XL47zN4ZKisb", "168.119.237.8"),
    (
        "12D3KooWL6yrrNACb7t7RPyTEPxKmq8jtrcbkcNd6H5G2hK7bXaL",
        "168.119.236.233",
    ),
    (
        "12D3KooWHBeCnJdzNk51G4mLnao9cDsjuqiMTEo5wMFXrd25bd1F",
        "168.119.236.243",
    ),
    (
        "12D3KooW9soGyPfX6kcyh3uVXNHq1y2dPmQNt2veKgdLXkBiCVKq",
        "168.119.236.246",
    ),
    ("12D3KooWPR2RoPi19vQtLugjCdvVmCcGLP2iXAzbDfP3tp81ZL4d", "168.119.237.13"),
    ("12D3KooWKu8pMTgteWacwFjN7zRWWHb3bctyTvHU3xx5x4x6qDYY", "195.201.91.96"),
    ("12D3KooWJWBnkVsVNjiqUEPjLyHpiSmQVAJ5t6qt1Txv5ctJi9Xd", "195.201.91.53"),
    (
        "12D3KooWGrUpCAbkxhPRioNs64sbUmPmpEcou6hYfrqQvxfWDEuf",
        "168.119.174.126",
    ),
    ("12D3KooWEaZpH61H4yuQkaNG5AsyGdpBhKRppaLdAY52a774ab5u", "46.4.78.11"),
    ("12D3KooWAd5gPXwX7eDvKWwkr2FZGfoJceKDCA53SHmTFFVkrN7Q", "46.4.87.18"),
];

#[cfg(target_arch = "wasm32")]
pub fn get_all_network_seednodes(_netid: u16) -> Vec<(PeerId, RelayAddress)> { Vec::new() }

#[cfg(not(target_arch = "wasm32"))]
pub fn get_all_network_seednodes(netid: u16) -> Vec<(PeerId, RelayAddress)> {
    use std::str::FromStr;

    if netid != NETID_7777 {
        return Vec::new();
    }
    ALL_NETID_7777_SEEDNODES
        .iter()
        .map(|(peer_id, ipv4)| {
            let peer_id = PeerId::from_str(*peer_id).expect("valid peer id");
            let address = RelayAddress::IPv4(ipv4.to_string());
            (peer_id, address)
        })
        .collect()
}
