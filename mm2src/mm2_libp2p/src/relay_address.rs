use crate::{NetworkInfo, NetworkPorts};
use derive_more::Display;
use libp2p::Multiaddr;
use serde::{de, Deserialize, Deserializer};
use std::str::FromStr;

#[derive(Debug, Display)]
pub enum RelayAddressError {
    #[display(
        fmt = "Error parsing 'RelayAddress' from {}: address has unknown protocol, expected either IPv4 or DNS or Memory address",
        found
    )]
    FromStrError { found: String },
    #[display(
        fmt = "Error converting '{:?}' to Multiaddr: unexpected IPv4/DNS address on a memory network",
        self_str
    )]
    DistributedAddrOnMemoryNetwork { self_str: String },
    #[display(
        fmt = "Error converting '{:?}' to Multiaddr: unexpected memory address on a distributed network",
        self_str
    )]
    MemoryAddrOnDistributedNetwork { self_str: String },
}

impl std::error::Error for RelayAddressError {}

impl RelayAddressError {
    fn distributed_addr_on_memory_network(addr: &RelayAddress) -> RelayAddressError {
        RelayAddressError::DistributedAddrOnMemoryNetwork {
            self_str: format!("{:?}", addr),
        }
    }

    fn memory_addr_on_distributed_network(addr: &RelayAddress) -> RelayAddressError {
        RelayAddressError::MemoryAddrOnDistributedNetwork {
            self_str: format!("{:?}", addr),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RelayAddress {
    IPv4(String),
    Dns(String),
    Memory(u64),
}

impl FromStr for RelayAddress {
    type Err = RelayAddressError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // check if the string is IPv4
        if std::net::Ipv4Addr::from_str(s).is_ok() {
            return Ok(RelayAddress::IPv4(s.to_string()));
        }
        // check if the string is a domain name
        if validate_domain_name(s) {
            return Ok(RelayAddress::Dns(s.to_owned()));
        }
        // check if the string is a `/memory/<PORT>` address
        if let Some(port_str) = s.strip_prefix("/memory/") {
            if let Ok(port) = port_str.parse() {
                return Ok(RelayAddress::Memory(port));
            }
        }
        Err(RelayAddressError::FromStrError { found: s.to_owned() })
    }
}

impl<'de> Deserialize<'de> for RelayAddress {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        let addr_str = String::deserialize(deserializer)?;
        RelayAddress::from_str(&addr_str).map_err(de::Error::custom)
    }
}

impl RelayAddress {
    /// Try to convert `RelayAddress` to `Multiaddr` using the given `network_info`.
    pub fn try_to_multiaddr(&self, network_info: NetworkInfo) -> Result<Multiaddr, RelayAddressError> {
        let network_ports = match network_info {
            NetworkInfo::InMemory => match self {
                RelayAddress::Memory(port) => return Ok(memory_multiaddr(*port)),
                _ => return Err(RelayAddressError::distributed_addr_on_memory_network(self)),
            },
            NetworkInfo::Distributed { network_ports } => network_ports,
        };

        match self {
            RelayAddress::IPv4(ipv4) => Ok(ipv4_multiaddr(&ipv4, network_ports)),
            RelayAddress::Dns(dns) => Ok(dns_multiaddr(&dns, network_ports)),
            RelayAddress::Memory(_) => Err(RelayAddressError::memory_addr_on_distributed_network(self)),
        }
    }
}

/// Use [this](https://regex101.com/r/94nCB5/1) regular expression to validate the domain name.
/// See examples at the linked resource above.
fn validate_domain_name(s: &str) -> bool {
    use regex::Regex;

    lazy_static! {
        static ref DNS_REGEX: Regex = Regex::new(r#"^([a-z0-9]+(-[a-z0-9]+)*\.)+[a-z]{2,}$"#).unwrap();
    }

    DNS_REGEX.is_match(s)
}

fn memory_multiaddr(port: u64) -> Multiaddr { format!("/memory/{}", port).parse().unwrap() }

#[cfg(target_arch = "wasm32")]
fn ipv4_multiaddr(ipv4_addr: &str, ports: NetworkPorts) -> Multiaddr {
    format!("/ip4/{}/tcp/{}/ws", ipv4_addr, ports.ws).parse().unwrap()
}

#[cfg(not(target_arch = "wasm32"))]
fn ipv4_multiaddr(ipv4_addr: &str, ports: NetworkPorts) -> Multiaddr {
    format!("/ip4/{}/tcp/{}", ipv4_addr, ports.tcp).parse().unwrap()
}

#[cfg(target_arch = "wasm32")]
fn dns_multiaddr(dns_addr: &str, ports: NetworkPorts) -> Multiaddr {
    format!("/dns/{}/tcp/{}/ws", dns_addr, ports.ws).parse().unwrap()
}

#[cfg(not(target_arch = "wasm32"))]
fn dns_multiaddr(dns_addr: &str, ports: NetworkPorts) -> Multiaddr {
    format!("/dns/{}/tcp/{}", dns_addr, ports.tcp).parse().unwrap()
}

#[test]
fn test_relay_address_from_str() {
    let valid_addresses = vec![
        ("127.0.0.1", RelayAddress::IPv4("127.0.0.1".to_owned())),
        ("255.255.255.255", RelayAddress::IPv4("255.255.255.255".to_owned())),
        ("google.com", RelayAddress::Dns("google.com".to_owned())),
        ("www.google.com", RelayAddress::Dns("www.google.com".to_owned())),
        ("g.co", RelayAddress::Dns("g.co".to_owned())),
        (
            "stackoverflow.co.uk",
            RelayAddress::Dns("stackoverflow.co.uk".to_owned()),
        ),
        ("1.2.3.4.com", RelayAddress::Dns("1.2.3.4.com".to_owned())),
        ("/memory/123", RelayAddress::Memory(123)),
        ("/memory/71428421981", RelayAddress::Memory(71428421981)),
    ];
    for (s, expected) in valid_addresses {
        let actual = RelayAddress::from_str(s).expect(&format!("Error parsing '{}'", s));
        assert_eq!(actual, expected);
    }

    let invalid_addresses = vec![
        "127.0.0",
        "127.0.0.0.2",
        "google.c",
        "http://google.com",
        "https://google.com/",
        "google.com/",
        "/memory/",
        "/memory/9999999999999999999999999999999",
    ];
    for s in invalid_addresses {
        let _ = RelayAddress::from_str(s).expect_err("Expected an error");
    }
}

#[test]
fn test_deserialize_relay_address() {
    #[derive(Deserialize, PartialEq)]
    struct Config {
        addresses: Vec<RelayAddress>,
    }

    let Config { addresses: actual } =
        serde_json::from_str(r#"{"addresses": ["foo.bar.com", "127.0.0.2", "/memory/12345"]}"#)
            .expect("Error deserializing a list of RelayAddress");
    let expected = vec![
        RelayAddress::Dns("foo.bar.com".to_owned()),
        RelayAddress::IPv4("127.0.0.2".to_owned()),
        RelayAddress::Memory(12345),
    ];
    assert_eq!(actual, expected);
}
