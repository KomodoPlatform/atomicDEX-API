/******************************************************************************
 * Copyright © 2014-2018 The SuperNET Developers.                             *
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
 ******************************************************************************/
//
//  rpc.rs
//
//  Copyright © 2014-2018 SuperNET. All rights reserved.
//

#[cfg(not(target_arch = "wasm32"))] use common::log::warn;
use common::log::{error, info};
use common::mm_ctx::MmArc;
use common::mm_error::prelude::*;
use common::{err_to_rpc_json_string, err_tp_rpc_json, HttpStatusCode};
use derive_more::Display;
use futures::future::{join_all, FutureExt};
use http::header::{HeaderValue, ACCESS_CONTROL_ALLOW_ORIGIN};
use http::request::Parts;
use http::{Method, Request, Response, StatusCode};
#[cfg(not(target_arch = "wasm32"))]
use hyper::{self, Body, Server};
use serde::Serialize;
use serde_json::{self as json, Value as Json};
use std::net::SocketAddr;

#[path = "rpc/dispatcher/dispatcher_legacy.rs"]
mod dispatcher_legacy;

#[path = "rpc/dispatcher/dispatcher_v2.rs"] mod dispatcher_v2;

#[path = "rpc/lp_commands.rs"] pub mod lp_commands;
#[path = "rpc/lp_protocol.rs"] mod lp_protocol;
use self::lp_protocol::{MmRpcBuilder, MmRpcResponse, MmRpcVersion};

/// Lists the RPC method not requiring the "userpass" authentication.  
/// None is also public to skip auth and display proper error in case of method is missing
const PUBLIC_METHODS: &[Option<&str>] = &[
    // Sorted alphanumerically (on the first letter) for readability.
    Some("fundvalue"),
    Some("getprice"),
    Some("getpeers"),
    Some("getcoins"),
    Some("help"),
    Some("metrics"),
    Some("notify"), // Manually checks the peer's public key.
    Some("orderbook"),
    Some("passphrase"), // Manually checks the "passphrase".
    Some("pricearray"),
    Some("psock"),
    Some("statsdisp"),
    Some("stats_swap_status"),
    Some("tradesarray"),
    Some("ticker"),
    None,
];

pub type DispatcherResult<T> = Result<T, MmError<DispatcherError>>;

#[derive(Display, Serialize, SerializeErrorType)]
#[serde(tag = "error_type", content = "error_data")]
pub enum DispatcherError {
    #[display(fmt = "No such method: {:?}", method)]
    NoSuchMethod { method: String },
    #[display(fmt = "Error parsing request: {}", _0)]
    InvalidRequest(String),
    #[display(fmt = "Selected method can be called from localhost only!")]
    LocalHostOnly,
    #[display(fmt = "Userpass is not set!")]
    UserpassIsNotSet,
    #[display(fmt = "Userpass is invalid!")]
    UserpassIsInvalid,
    #[display(fmt = "Error parsing mmrpc version: {}", _0)]
    InvalidMmRpcVersion(String),
}

impl HttpStatusCode for DispatcherError {
    fn status_code(&self) -> StatusCode {
        match self {
            DispatcherError::NoSuchMethod { .. }
            | DispatcherError::InvalidRequest(_)
            | DispatcherError::InvalidMmRpcVersion(_) => StatusCode::BAD_REQUEST,
            DispatcherError::LocalHostOnly | DispatcherError::UserpassIsNotSet | DispatcherError::UserpassIsInvalid => {
                StatusCode::FORBIDDEN
            },
        }
    }
}

impl From<serde_json::Error> for DispatcherError {
    fn from(e: serde_json::Error) -> Self { DispatcherError::InvalidRequest(e.to_string()) }
}

#[allow(unused_macros)]
macro_rules! unwrap_or_err_response {
    ($e:expr, $($args:tt)*) => {
        match $e {
            Ok(ok) => ok,
            Err(err) => return rpc_err_response(500, &ERRL!("{}", err)),
        }
    };
}

async fn process_json_batch_requests(ctx: MmArc, requests: &[Json], client: SocketAddr) -> Result<Json, String> {
    let mut futures = Vec::with_capacity(requests.len());
    for request in requests {
        futures.push(process_single_request(ctx.clone(), request.clone(), client));
    }
    let results = join_all(futures).await;
    let responses: Vec<_> = results
        .into_iter()
        .map(|resp| match resp {
            Ok(r) => match json::from_slice(r.body()) {
                Ok(j) => j,
                Err(e) => {
                    error!("Response {:?} is not a valid JSON, error: {}", r, e);
                    Json::Null
                },
            },
            Err(e) => err_tp_rpc_json(e),
        })
        .collect();
    Ok(Json::Array(responses))
}

#[cfg(target_arch = "wasm32")]
async fn process_json_request(ctx: MmArc, req_json: Json, client: SocketAddr) -> Result<Json, String> {
    if let Some(requests) = req_json.as_array() {
        return process_json_batch_requests(ctx, &requests, client)
            .await
            .map_err(|e| ERRL!("{}", e));
    }

    let r = try_s!(process_single_request(ctx, req_json, client).await);
    json::from_slice(r.body()).map_err(|e| ERRL!("Response {:?} is not a valid JSON, error: {}", r, e))
}

#[cfg(not(target_arch = "wasm32"))]
async fn process_json_request(ctx: MmArc, req_json: Json, client: SocketAddr) -> Result<Response<Vec<u8>>, String> {
    if let Some(requests) = req_json.as_array() {
        let response = try_s!(process_json_batch_requests(ctx, &requests, client).await);
        let res = try_s!(json::to_vec(&response));
        return Ok(try_s!(Response::builder().body(res)));
    }

    process_single_request(ctx, req_json, client).await
}

fn response_from_dispatcher_error(
    error: MmError<DispatcherError>,
    version: MmRpcVersion,
    id: Option<usize>,
) -> Response<Vec<u8>> {
    error!("RPC dispatcher error: {}", error);
    let response: MmRpcResponse<(), _> = MmRpcBuilder::err(error).version(version).id(id).build();
    response.serialize_http_response()
}

async fn process_single_request(ctx: MmArc, req: Json, client: SocketAddr) -> Result<Response<Vec<u8>>, String> {
    let local_only = ctx.conf["rpc_local_only"].as_bool().unwrap_or(true);
    if req["mmrpc"].is_null() {
        return dispatcher_legacy::process_single_request(ctx, req, client, local_only)
            .await
            .map_err(|e| ERRL!("{}", e));
    }

    let id = req["id"].as_u64().map(|id| id as usize);
    let version: MmRpcVersion = match json::from_value(req["mmrpc"].clone()) {
        Ok(v) => v,
        Err(e) => {
            let error = MmError::new(DispatcherError::InvalidMmRpcVersion(e.to_string()));
            // use the latest `MmRpcVersion` if the version is not recognized
            return Ok(response_from_dispatcher_error(error, MmRpcVersion::V2, id));
        },
    };

    match dispatcher_v2::process_single_request(ctx, req, client, local_only).await {
        Ok(response) => Ok(response),
        Err(e) => {
            // return always serialized response
            return Ok(response_from_dispatcher_error(e, version, id));
        },
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn rpc_service(req: Request<Body>, ctx_h: u32, client: SocketAddr) -> Response<Body> {
    /// Unwraps a result or propagates its error 500 response with the specified headers (if they are present).
    macro_rules! try_sf {
        ($value: expr $(, $header_key:expr => $header_val:expr)*) => {
            match $value {
                Ok(ok) => ok,
                Err(err) => {
                    error!("RPC error response: {}", err);
                    let ebody = err_to_rpc_json_string(&fomat!((err)));
                    // generate a `Response` with the headers specified in `$header_key` and `$header_val`
                    let response = Response::builder().status(500) $(.header($header_key, $header_val))* .body(Body::from(ebody)).unwrap();
                    return response;
                },
            }
        };
    }

    async fn process_rpc_request(
        ctx: MmArc,
        req: Parts,
        req_json: Json,
        client: SocketAddr,
    ) -> Result<Response<Vec<u8>>, String> {
        if req.method != Method::POST {
            return ERR!("Only POST requests are supported!");
        }

        process_json_request(ctx, req_json, client).await
    }

    let ctx = try_sf!(MmArc::from_ffi_handle(ctx_h));
    // https://github.com/artemii235/SuperNET/issues/219
    let rpc_cors = match ctx.conf["rpccors"].as_str() {
        Some(s) => try_sf!(HeaderValue::from_str(s)),
        None => HeaderValue::from_static("http://localhost:3000"),
    };

    // Convert the native Hyper stream into a portable stream of `Bytes`.
    let (req, req_body) = req.into_parts();
    let req_bytes = try_sf!(hyper::body::to_bytes(req_body).await, ACCESS_CONTROL_ALLOW_ORIGIN => rpc_cors);
    let req_json: Json = try_sf!(json::from_slice(&req_bytes), ACCESS_CONTROL_ALLOW_ORIGIN => rpc_cors);

    let res = try_sf!(process_rpc_request(ctx, req, req_json, client).await, ACCESS_CONTROL_ALLOW_ORIGIN => rpc_cors);
    let (mut parts, body) = res.into_parts();
    parts.headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, rpc_cors);
    Response::from_parts(parts, Body::from(body))
}

#[cfg(not(target_arch = "wasm32"))]
pub extern "C" fn spawn_rpc(ctx_h: u32) {
    use common::wio::CORE;
    use hyper::server::conn::AddrStream;
    use hyper::service::{make_service_fn, service_fn};
    use std::convert::Infallible;

    // NB: We need to manually handle the incoming connections in order to get the remote IP address,
    // cf. https://github.com/hyperium/hyper/issues/1410#issuecomment-419510220.
    // Although if the ability to access the remote IP address is solved by the Hyper in the future
    // then we might want to refactor into starting it ideomatically in order to benefit from a more graceful shutdown,
    // cf. https://github.com/hyperium/hyper/pull/1640.

    let ctx = MmArc::from_ffi_handle(ctx_h).expect("No context");

    let rpc_ip_port = ctx.rpc_ip_port().unwrap();
    // By entering the context, we tie `tokio::spawn` to this executor.
    let _runtime_guard = CORE.0.enter();

    let server = Server::try_bind(&rpc_ip_port).unwrap_or_else(|_| panic!("Can't bind on {}", rpc_ip_port));
    let make_svc = make_service_fn(move |socket: &AddrStream| {
        let remote_addr = socket.remote_addr();
        async move {
            Ok::<_, Infallible>(service_fn(move |req: Request<Body>| async move {
                let res = rpc_service(req, ctx_h, remote_addr).await;
                Ok::<_, Infallible>(res)
            }))
        }
    });

    let (shutdown_tx, shutdown_rx) = futures::channel::oneshot::channel::<()>();
    let mut shutdown_tx = Some(shutdown_tx);
    ctx.on_stop(Box::new(move || {
        if let Some(shutdown_tx) = shutdown_tx.take() {
            info!("on_stop] firing shutdown_tx!");
            if shutdown_tx.send(()).is_err() {
                warn!("on_stop] shutdown_tx already closed");
            }
            Ok(())
        } else {
            ERR!("on_stop callback called twice!")
        }
    }));

    let server = server
        .http1_half_close(false)
        .serve(make_svc)
        .with_graceful_shutdown(shutdown_rx.then(|_| futures::future::ready(())));

    let server = server.then(|r| {
        if let Err(err) = r {
            error!("{}", err);
        };
        futures::future::ready(())
    });

    let rpc_ip_port = ctx.rpc_ip_port().unwrap();
    CORE.0.spawn({
        log_tag!(
            ctx,
            "😉";
            fmt = ">>>>>>>>>> DEX stats {}:{} DEX stats API enabled at unixtime.{}  <<<<<<<<<",
            rpc_ip_port.ip(),
            rpc_ip_port.port(),
            gstuff::now_ms() / 1000
        );
        let _ = ctx.rpc_started.pin(true);
        server
    });
}

#[cfg(target_arch = "wasm32")]
pub fn spawn_rpc(ctx_h: u32) {
    use common::wasm_rpc;
    use futures::StreamExt;
    use std::sync::Mutex;

    let ctx = MmArc::from_ffi_handle(ctx_h).expect("No context");
    if ctx.wasm_rpc.is_some() {
        error!("RPC is initialized already");
        return;
    }

    let client: SocketAddr = "127.0.0.1:1"
        .parse()
        .expect("'127.0.0.1:1' must be valid socket address");

    let (request_tx, mut request_rx) = wasm_rpc::channel();
    let ctx_c = ctx.clone();
    let fut = async move {
        while let Some((request_json, response_tx)) = request_rx.next().await {
            let response = process_json_request(ctx_c.clone(), request_json, client).await;
            if let Err(e) = response_tx.send(response) {
                error!("Response is not processed: {:?}", e);
            }
        }
    };
    common::executor::spawn(fut);

    // even if the [`MmCtx::wasm_rpc`] is initialized already, the spawned future above will be shutdown
    if let Err(e) = ctx.wasm_rpc.pin(request_tx) {
        error!("'MmCtx::wasm_rpc' is initialized already: {}", e);
        return;
    };
    if let Err(e) = ctx.rpc_started.pin(true) {
        error!("'MmCtx::rpc_started' is set already: {}", e);
        return;
    }

    log_tag!(
        ctx,
        "😉";
        fmt = ">>>>>>>>>> DEX stats API enabled at unixtime.{}  <<<<<<<<<",
        common::now_ms() / 1000
    );
}
