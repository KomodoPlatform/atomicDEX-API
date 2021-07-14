use super::{spawn_gossipsub, AdexBehaviourCmd, AdexBehaviourEvent, AdexResponse, NodeType, RelayAddress};
use async_std::task::spawn;
use futures::channel::{mpsc, oneshot};
use futures::{Future, SinkExt, StreamExt};
use libp2p::PeerId;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

static TEST_LISTEN_PORT: AtomicU64 = AtomicU64::new(1);

fn next_port() -> u64 { TEST_LISTEN_PORT.fetch_add(1, Ordering::Relaxed) }

fn spawn_boxed(fut: Box<dyn Future<Output = ()> + Send + Unpin + 'static>) { spawn(fut); }

struct Node {
    peer_id: PeerId,
    cmd_tx: mpsc::Sender<AdexBehaviourCmd>,
}

impl Node {
    async fn spawn<F>(port: u64, seednodes: Vec<u64>, on_event: F) -> Node
    where
        F: Fn(mpsc::Sender<AdexBehaviourCmd>, AdexBehaviourEvent) + Send + 'static,
    {
        let node_type = NodeType::RelayInMemory { port };
        let seednodes = seednodes.into_iter().map(RelayAddress::Memory).collect();
        let (cmd_tx, mut event_rx, peer_id, _) = spawn_gossipsub(333, None, spawn_boxed, seednodes, node_type, |_| {})
            .await
            .expect("Error spawning AdexBehaviour");

        // spawn a response future
        let cmd_tx_fut = cmd_tx.clone();
        spawn(async move {
            loop {
                let cmd_tx_fut = cmd_tx_fut.clone();
                match event_rx.next().await {
                    Some(r) => on_event(cmd_tx_fut, r),
                    _ => {
                        println!("Finish response future");
                        break;
                    },
                }
            }
        });

        Node { peer_id, cmd_tx }
    }

    async fn send_cmd(&mut self, cmd: AdexBehaviourCmd) { self.cmd_tx.send(cmd).await.unwrap(); }

    async fn wait_peers(&mut self, number: usize) {
        let mut attempts = 0;
        loop {
            let (tx, rx) = oneshot::channel();
            self.cmd_tx
                .send(AdexBehaviourCmd::GetPeersInfo { result_tx: tx })
                .await
                .unwrap();
            match rx.await {
                Ok(map) => {
                    if map.len() >= number {
                        return;
                    }
                    async_std::task::sleep(Duration::from_millis(500)).await;
                },
                Err(e) => panic!("{}", e),
            }
            attempts += 1;
            if attempts >= 10 {
                panic!("wait_peers {} attempts exceeded", attempts);
            }
        }
    }
}

#[tokio::test]
async fn test_request_response_ok() {
    let _ = env_logger::try_init();

    let request_received = Arc::new(AtomicBool::new(false));
    let request_received_cpy = request_received.clone();

    let node1_port = next_port();
    let node1 = Node::spawn(node1_port, vec![], move |mut cmd_tx, event| {
        let (request, response_channel) = match event {
            AdexBehaviourEvent::PeerRequest {
                request,
                response_channel,
                ..
            } => (request, response_channel),
            _ => return,
        };

        request_received_cpy.store(true, Ordering::Relaxed);
        assert_eq!(request, b"test request");

        let res = AdexResponse::Ok {
            response: b"test response".to_vec(),
        };
        cmd_tx
            .try_send(AdexBehaviourCmd::SendResponse { res, response_channel })
            .unwrap();
    })
    .await;

    let mut node2 = Node::spawn(next_port(), vec![node1_port], |_, _| ()).await;

    node2.wait_peers(1).await;

    let (response_tx, response_rx) = oneshot::channel();
    node2
        .send_cmd(AdexBehaviourCmd::RequestAnyRelay {
            req: b"test request".to_vec(),
            response_tx,
        })
        .await;

    let response = response_rx.await.unwrap();
    assert_eq!(response, Some((node1.peer_id, b"test response".to_vec())));

    assert!(request_received.load(Ordering::Relaxed));
}

#[tokio::test]
async fn test_request_response_ok_three_peers() {
    let _ = env_logger::try_init();

    #[derive(Default)]
    struct RequestHandler {
        requests: u8,
    }

    impl RequestHandler {
        fn handle(&mut self, mut cmd_tx: mpsc::Sender<AdexBehaviourCmd>, event: AdexBehaviourEvent) {
            let (request, response_channel) = match event {
                AdexBehaviourEvent::PeerRequest {
                    request,
                    response_channel,
                    ..
                } => (request, response_channel),
                _ => return,
            };

            self.requests += 1;

            assert_eq!(request, b"test request");

            // the first time we should respond the none
            if self.requests == 1 {
                let res = AdexResponse::None;
                cmd_tx
                    .try_send(AdexBehaviourCmd::SendResponse { res, response_channel })
                    .unwrap();
                return;
            }

            // the second time we should respond an error
            if self.requests == 2 {
                let res = AdexResponse::Err {
                    error: "test error".into(),
                };
                cmd_tx
                    .try_send(AdexBehaviourCmd::SendResponse { res, response_channel })
                    .unwrap();
                return;
            }

            // the third time we should respond an ok
            if self.requests == 3 {
                let res = AdexResponse::Ok {
                    response: format!("success {} request", self.requests).as_bytes().to_vec(),
                };
                cmd_tx
                    .try_send(AdexBehaviourCmd::SendResponse { res, response_channel })
                    .unwrap();
                return;
            }

            panic!("Request received more than 3 times");
        }
    }

    let request_handler = Arc::new(Mutex::new(RequestHandler::default()));

    let mut receivers = Vec::new();
    for _ in 0..3 {
        let handler = request_handler.clone();
        let receiver_port = next_port();
        let receiver = Node::spawn(receiver_port, vec![], move |cmd_tx, event| {
            let mut handler = handler.lock().unwrap();
            handler.handle(cmd_tx, event)
        })
        .await;
        receivers.push((receiver_port, receiver));
    }

    let mut sender = Node::spawn(
        next_port(),
        receivers.iter().map(|(port, _)| *port).collect(),
        |_, _| (),
    )
    .await;

    sender.wait_peers(3).await;

    let (response_tx, response_rx) = oneshot::channel();
    sender
        .send_cmd(AdexBehaviourCmd::RequestAnyRelay {
            req: b"test request".to_vec(),
            response_tx,
        })
        .await;

    let (_peer_id, res) = response_rx.await.unwrap().unwrap();
    assert_eq!(res, b"success 3 request".to_vec());
}

#[tokio::test]
async fn test_request_response_none() {
    let _ = env_logger::try_init();

    let request_received = Arc::new(AtomicBool::new(false));
    let request_received_cpy = request_received.clone();

    let node1_port = next_port();
    let _node1 = Node::spawn(node1_port, vec![], move |mut cmd_tx, event| {
        let (request, response_channel) = match event {
            AdexBehaviourEvent::PeerRequest {
                request,
                response_channel,
                ..
            } => (request, response_channel),
            _ => return,
        };

        request_received_cpy.store(true, Ordering::Relaxed);
        assert_eq!(request, b"test request");

        let res = AdexResponse::None;
        cmd_tx
            .try_send(AdexBehaviourCmd::SendResponse { res, response_channel })
            .unwrap();
    })
    .await;

    let mut node2 = Node::spawn(next_port(), vec![node1_port], |_, _| ()).await;

    node2.wait_peers(1).await;

    let (response_tx, response_rx) = oneshot::channel();
    node2
        .send_cmd(AdexBehaviourCmd::RequestAnyRelay {
            req: b"test request".to_vec(),
            response_tx,
        })
        .await;

    assert_eq!(response_rx.await.unwrap(), None);
    assert!(request_received.load(Ordering::Relaxed));
}

#[tokio::test]
async fn test_request_peers_ok_three_peers() {
    let _ = env_logger::try_init();

    let receiver1_port = next_port();
    let receiver1 = Node::spawn(receiver1_port, vec![], move |mut cmd_tx, event| {
        let (request, response_channel) = match event {
            AdexBehaviourEvent::PeerRequest {
                request,
                response_channel,
                ..
            } => (request, response_channel),
            _ => return,
        };

        assert_eq!(request, b"test request");

        let res = AdexResponse::None;
        cmd_tx
            .try_send(AdexBehaviourCmd::SendResponse { res, response_channel })
            .unwrap();
    })
    .await;

    let receiver2_port = next_port();
    let receiver2 = Node::spawn(receiver2_port, vec![], move |mut cmd_tx, event| {
        let (request, response_channel) = match event {
            AdexBehaviourEvent::PeerRequest {
                request,
                response_channel,
                ..
            } => (request, response_channel),
            _ => return,
        };

        assert_eq!(request, b"test request");

        let res = AdexResponse::Err {
            error: "test error".into(),
        };
        cmd_tx
            .try_send(AdexBehaviourCmd::SendResponse { res, response_channel })
            .unwrap();
    })
    .await;

    let receiver3_port = next_port();
    let receiver3 = Node::spawn(receiver3_port, vec![], move |mut cmd_tx, event| {
        let (request, response_channel) = match event {
            AdexBehaviourEvent::PeerRequest {
                request,
                response_channel,
                ..
            } => (request, response_channel),
            _ => return,
        };

        assert_eq!(request, b"test request");

        let res = AdexResponse::Ok {
            response: b"test response".to_vec(),
        };
        cmd_tx
            .try_send(AdexBehaviourCmd::SendResponse { res, response_channel })
            .unwrap();
    })
    .await;
    let mut sender = Node::spawn(
        next_port(),
        vec![receiver1_port, receiver2_port, receiver3_port],
        |_, _| (),
    )
    .await;

    sender.wait_peers(3).await;

    let (response_tx, response_rx) = oneshot::channel();
    sender
        .send_cmd(AdexBehaviourCmd::RequestRelays {
            req: b"test request".to_vec(),
            response_tx,
        })
        .await;

    let mut expected = vec![
        (receiver1.peer_id, AdexResponse::None),
        (receiver2.peer_id, AdexResponse::Err {
            error: "test error".into(),
        }),
        (receiver3.peer_id, AdexResponse::Ok {
            response: b"test response".to_vec(),
        }),
    ];
    expected.sort_by(|x, y| x.0.cmp(&y.0));

    let mut responses = response_rx.await.unwrap();
    responses.sort_by(|x, y| x.0.cmp(&y.0));
    assert_eq!(responses, expected);
}
