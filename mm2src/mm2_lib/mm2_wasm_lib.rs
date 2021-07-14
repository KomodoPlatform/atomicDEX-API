//! Some specifics of using the [`wasm_bindgen`] library:
//!
//! # Currently only `Result<T, JsValue>` is allowed
//! [tracking issue]: https://github.com/rustwasm/wasm-bindgen/issues/1004
//!
//! # JavaScript enums do not support methods at all
//! [tracking issue]: https://github.com/rustwasm/wasm-bindgen/issues/1715
//!
//! # WASM is currently single-threaded
//! There is very few types in [`wasm_bindgen`] crate that are `Send` and `Sync`.
//! Although wasm is currently single-threaded and it's possible to create a wrapper type and then implement `Send` and `Sync`,
//! but it won't be safe when wasm becomes multi-threaded.
//! [blogpost]: https://rustwasm.github.io/2018/10/24/multithreading-rust-and-wasm.html

use super::*;
use crate::mm2::LpMainParams;
use common::log::{register_callback, LogLevel, WasmCallback};
use common::wasm_rpc::WasmRpcResponse;
use common::{executor, set_panic_hook};
use gstuff::any_to_str;
use js_sys::Array;
use num_traits::FromPrimitive;
use serde_json::{self as json, Value as Json};
use std::cell::RefCell;
use std::sync::Mutex;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

/// The errors can be thrown when using the `mm2_main` function incorrectly.
#[wasm_bindgen]
#[derive(Primitive)]
pub enum Mm2MainErr {
    AlreadyRuns = 1,
    InvalidParams = 2,
    NoCoinsInConf = 3,
}

impl From<Mm2MainErr> for JsValue {
    fn from(e: Mm2MainErr) -> Self { JsValue::from(e as i32) }
}

#[derive(Deserialize)]
struct MainParams {
    conf: Json,
    log_level: LogLevel,
}

impl From<MainParams> for LpMainParams {
    fn from(orig: MainParams) -> Self { LpMainParams::with_conf(orig.conf).log_filter(orig.log_level) }
}

/// Run the MarketMaker2.
///
/// # Parameters
///
/// * `conf` is a UTF-8 string JSON.
/// * `log_cb` is a JS function with the following signature:
/// ```typescript
/// function(level: number, line: string)
/// ```
///
/// # Usage
///
/// ```javascript
/// import init, {mm2_main, LogLevel, Mm2MainErr} from "./path/to/mm2.js";
///
/// const params = {
///     conf: { "gui":"WASMTEST", mm2:1, "passphrase":"YOUR_PASSPHRASE_HERE", "rpc_password":"test123", "coins":[{"coin":"ETH","protocol":{"type":"ETH"}}] },
///     log_level: LogLevel.Info,
/// };
/// let handle_log = function (_level, line) { console.log(line) };
/// try {
///     mm2_main(params, handle_log);
/// } catch (e) {
///     switch (e) {
///         case Mm2MainErr.AlreadyRuns:
///             alert("MarketMaker2 already runs...");
///             break;
///         // handle other errors...
///         default:
///             alert(`Unexpected error: ${e}`);
///             break;
///     }
/// }
/// ```
#[wasm_bindgen]
pub fn mm2_main(params: JsValue, log_cb: js_sys::Function) -> Result<(), JsValue> {
    let params: MainParams = match params.into_serde() {
        Ok(p) => p,
        Err(e) => {
            console_err!("Expected 'MainParams' as the first argument, found {:?}: {}", params, e);
            return Err(Mm2MainErr::InvalidParams.into());
        },
    };
    if params.conf["coins"].is_null() {
        console_err!("Config must contain 'coins' field: {:?}", params.conf);
        return Err(Mm2MainErr::NoCoinsInConf.into());
    }
    let params = LpMainParams::from(params);

    if LP_MAIN_RUNNING.load(Ordering::Relaxed) {
        return Err(Mm2MainErr::AlreadyRuns.into());
    }
    CTX.store(0, Ordering::Relaxed); // Remove the old context ID during restarts.

    register_callback(WasmCallback::with_js_function(log_cb));
    set_panic_hook();

    let fut = async move {
        if let Err(true) = LP_MAIN_RUNNING.compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed) {
            console_err!("lp_main already started!");
            return;
        }
        let ctx_cb = |ctx| CTX.store(ctx, Ordering::Relaxed);
        // TODO figure out how to use catch_unwind here
        // use futures::FutureExt;
        // match mm2::lp_main(params, &ctx_cb).catch_unwind().await {
        //     Ok(Ok(_)) => console_info!("run_lp_main finished"),
        //     Ok(Err(err)) => console_err!("run_lp_main error: {}", err),
        //     Err(err) => console_err!("run_lp_main panic: {:?}", any_to_str(&*err)),
        // };
        match mm2::lp_main(params, &ctx_cb).await {
            Ok(()) => console_info!("run_lp_main finished"),
            Err(err) => console_err!("run_lp_main error: {}", err),
        };
        LP_MAIN_RUNNING.store(false, Ordering::Relaxed)
    };
    executor::spawn_local(fut);

    Ok(())
}

/// Get the MarketMaker2 status.
#[wasm_bindgen]
pub fn mm2_main_status() -> MainStatus { mm2_status() }

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum Mm2RpcResponse {
    Ok(Json),
    Err { error: String },
}

impl From<WasmRpcResponse> for Mm2RpcResponse {
    fn from(response: WasmRpcResponse) -> Self {
        match response {
            Ok(payload) => Mm2RpcResponse::Ok(payload),
            Err(error) => Mm2RpcResponse::Err { error },
        }
    }
}

/// The errors can be thrown when using the `mm2_rpc` function incorrectly.
#[wasm_bindgen]
#[derive(Primitive)]
pub enum Mm2RpcErr {
    NotRunning = 1,
    InvalidPayload = 2,
    InternalError = 3,
}

impl From<Mm2RpcErr> for JsValue {
    fn from(e: Mm2RpcErr) -> Self { JsValue::from(e as i32) }
}

/// Invoke an RPC request.
///
/// # Parameters
///
/// * `payload` is a UTF-8 string JSON.
///
/// # Usage
///
/// ```javascript
/// import init, {mm2_rpc, Mm2RpcErr} from "./path/to/mm2.js";
///
/// async function version () {
///     try {
///         const payload = {
///             "userpass": "test123",
///             "method": "version",
///         };
///         const response = mm2_rpc(payload).await;
///         return response.result;
///     } catch (e) {
///         switch (e) {
///             case Mm2RpcErr.NotRunning:
///                 alert("MarketMaker2 not running yet...");
///                 break;
///             // handle other errors...
///             default:
///                 alert(`Unexpected error: ${e}`);
///                 break;
///         }
///     }
/// }
///
/// ```
#[wasm_bindgen]
pub async fn mm2_rpc(payload: JsValue) -> Result<JsValue, JsValue> {
    let request_json: Json = match payload.into_serde() {
        Ok(p) => p,
        Err(e) => {
            console_err!("Payload is not a valid JSON: {}", e);
            return Err(Mm2RpcErr::InvalidPayload.into());
        },
    };

    if !LP_MAIN_RUNNING.load(Ordering::Relaxed) {
        return Err(Mm2RpcErr::NotRunning.into());
    }

    let ctx = CTX.load(Ordering::Relaxed);
    if ctx == 0 {
        return Err(Mm2RpcErr::NotRunning.into());
    }

    let ctx = match MmArc::from_ffi_handle(ctx) {
        Ok(ctx) => ctx,
        Err(_) => return Err(Mm2RpcErr::NotRunning.into()),
    };

    let wasm_rpc = ctx.wasm_rpc.ok_or(JsValue::from(Mm2RpcErr::NotRunning))?;
    let response: Mm2RpcResponse = wasm_rpc.request(request_json).await.into();

    JsValue::from_serde(&response).map_err(|e| {
        console_err!("Couldn't represent the response '{:?}' as a JsValue: {}", response, e);
        JsValue::from(Mm2RpcErr::InternalError)
    })
}
