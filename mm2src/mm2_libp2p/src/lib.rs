#![feature(ip)]

#[macro_use] extern crate lazy_static;

mod adex_ping;
pub mod atomicdex_behaviour;
mod network;
mod peers_exchange;
pub mod relay_address;
pub mod request_response;
mod runtime;

use lazy_static::lazy_static;
use secp256k1::{Message as SecpMessage, PublicKey as Secp256k1Pubkey, Secp256k1, SecretKey, SignOnly, Signature,
                VerifyOnly};
use sha2::{Digest, Sha256};

pub use atomicdex_behaviour::{spawn_gossipsub, AdexBehaviourError, NodeType};
pub use atomicdex_gossipsub::{GossipsubEvent, GossipsubMessage, MessageId};
pub use libp2p::{Multiaddr, PeerId};
pub use relay_address::{RelayAddress, RelayAddressError};
use serde::{de, Deserialize, Serialize, Serializer};

lazy_static! {
    static ref SECP_VERIFY: Secp256k1<VerifyOnly> = Secp256k1::verification_only();
    static ref SECP_SIGN: Secp256k1<SignOnly> = Secp256k1::signing_only();
}

#[derive(Clone, Copy)]
pub enum NetworkInfo {
    /// The in-memory network.
    InMemory,
    /// The distributed network (out of the app memory).
    Distributed { network_ports: NetworkPorts },
}

impl NetworkInfo {
    pub fn in_memory(&self) -> bool { matches!(self, NetworkInfo::InMemory) }
}

#[derive(Clone, Copy)]
pub struct NetworkPorts {
    pub tcp: u16,
    pub ws: u16,
}

pub fn encode_message<T: Serialize>(message: &T) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    rmp_serde::to_vec(message)
}

pub fn decode_message<'de, T: de::Deserialize<'de>>(bytes: &'de [u8]) -> Result<T, rmp_serde::decode::Error> {
    rmp_serde::from_read_ref(bytes)
}

#[derive(Deserialize, Serialize)]
struct SignedMessageSerdeHelper<'a> {
    pubkey: PublicKey,
    #[serde(with = "serde_bytes")]
    signature: &'a [u8],
    #[serde(with = "serde_bytes")]
    payload: &'a [u8],
}

pub fn encode_and_sign<T: Serialize>(message: &T, secret: &[u8; 32]) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    let secret = SecretKey::from_slice(secret).unwrap();
    let encoded = encode_message(message)?;
    let sig_hash = SecpMessage::from_slice(&sha256(&encoded)).expect("Message::from_slice should never fail");
    let sig = SECP_SIGN.sign(&sig_hash, &secret);
    let serialized_sig = sig.serialize_compact();
    let pubkey = PublicKey::from(Secp256k1Pubkey::from_secret_key(&*SECP_SIGN, &secret));
    let msg = SignedMessageSerdeHelper {
        pubkey,
        signature: &serialized_sig,
        payload: &encoded,
    };
    encode_message(&msg)
}

pub fn decode_signed<'de, T: de::Deserialize<'de>>(
    encoded: &'de [u8],
) -> Result<(T, Signature, PublicKey), rmp_serde::decode::Error> {
    let helper: SignedMessageSerdeHelper = decode_message(encoded)?;
    let signature = Signature::from_compact(helper.signature)
        .map_err(|e| rmp_serde::decode::Error::Syntax(format!("Failed to parse signature {}", e)))?;
    let sig_hash = SecpMessage::from_slice(&sha256(&helper.payload)).expect("Message::from_slice should never fail");
    match &helper.pubkey {
        PublicKey::Secp256k1(serialized_pub) => {
            if SECP_VERIFY.verify(&sig_hash, &signature, &serialized_pub.0).is_err() {
                return Err(rmp_serde::decode::Error::Syntax("Invalid message signature".into()));
            }
        },
    }

    let payload: T = decode_message(helper.payload)?;
    Ok((payload, signature, helper.pubkey))
}

fn sha256(input: impl AsRef<[u8]>) -> [u8; 32] { Sha256::new().chain(input).finalize().into() }

#[derive(Debug, Eq, PartialEq)]
pub struct Secp256k1PubkeySerialize(Secp256k1Pubkey);

impl Serialize for Secp256k1PubkeySerialize {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0.serialize())
    }
}

impl<'de> de::Deserialize<'de> for Secp256k1PubkeySerialize {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as de::Deserializer<'de>>::Error>
    where
        D: de::Deserializer<'de>,
    {
        let slice: &[u8] = de::Deserialize::deserialize(deserializer)?;
        let pubkey =
            Secp256k1Pubkey::from_slice(slice).map_err(|e| de::Error::custom(format!("Error {} parsing pubkey", e)))?;

        Ok(Secp256k1PubkeySerialize(pubkey))
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum PublicKey {
    Secp256k1(Secp256k1PubkeySerialize),
}

impl PublicKey {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            PublicKey::Secp256k1(pubkey) => pubkey.0.serialize().to_vec(),
        }
    }

    pub fn to_hex(&self) -> String {
        match self {
            PublicKey::Secp256k1(pubkey) => hex::encode(pubkey.0.serialize().as_ref()),
        }
    }

    pub fn unprefixed(&self) -> [u8; 32] {
        let mut res = [0; 32];
        match self {
            PublicKey::Secp256k1(pubkey) => res.copy_from_slice(&pubkey.0.serialize()[1..33]),
        }
        res
    }
}

impl From<Secp256k1Pubkey> for PublicKey {
    fn from(pubkey: Secp256k1Pubkey) -> Self { PublicKey::Secp256k1(Secp256k1PubkeySerialize(pubkey)) }
}

pub type TopicPrefix = &'static str;
pub const TOPIC_SEPARATOR: char = '/';

pub fn pub_sub_topic(prefix: TopicPrefix, topic: &str) -> String {
    let mut res = prefix.to_owned();
    res.push(TOPIC_SEPARATOR);
    res.push_str(topic);
    res
}

#[test]
fn signed_message_serde() {
    let secret = [1u8; 32];
    let initial_msg = vec![0u8; 32];
    let signed_encoded = encode_and_sign(&initial_msg, &secret).unwrap();

    let (decoded, ..) = decode_signed::<Vec<u8>>(&signed_encoded).unwrap();
    assert_eq!(decoded, initial_msg);
}
