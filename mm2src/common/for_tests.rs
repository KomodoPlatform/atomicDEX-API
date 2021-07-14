//! Helpers used in the unit and integration tests.

use bigdecimal::BigDecimal;
use chrono::{Local, TimeZone};
use http::{HeaderMap, StatusCode};
use rand::Rng;
use serde_json::{self as json, Value as Json};
use std::collections::HashMap;
use std::net::IpAddr;
use std::path::PathBuf;
use std::process::Child;
use std::sync::Mutex;

use crate::executor::Timer;
use crate::mm_ctx::MmArc;
use crate::mm_metrics::{MetricType, MetricsJson};
use crate::now_float;

cfg_wasm32! {
    use crate::log::LogLevel;
    use crate::helperᶜ;
}

cfg_native! {
    use crate::block_on;
    use crate::log::dashboard_path;
    use crate::slurp;
    use crate::wio::{slurp_req, POOL};
    use bytes::Bytes;
    use futures::channel::oneshot;
    use futures::task::SpawnExt;
    use gstuff::ISATTY;
    use http::Request;
    use regex::Regex;
    use std::env;
    use std::fs;
    use std::io::Write;
    use std::net::Ipv4Addr;
    use std::path::Path;
    use std::process::Command;
}

pub const MAKER_SUCCESS_EVENTS: [&str; 11] = [
    "Started",
    "Negotiated",
    "TakerFeeValidated",
    "MakerPaymentSent",
    "TakerPaymentReceived",
    "TakerPaymentWaitConfirmStarted",
    "TakerPaymentValidatedAndConfirmed",
    "TakerPaymentSpent",
    "TakerPaymentSpendConfirmStarted",
    "TakerPaymentSpendConfirmed",
    "Finished",
];

pub const MAKER_ERROR_EVENTS: [&str; 13] = [
    "StartFailed",
    "NegotiateFailed",
    "TakerFeeValidateFailed",
    "MakerPaymentTransactionFailed",
    "MakerPaymentDataSendFailed",
    "MakerPaymentWaitConfirmFailed",
    "TakerPaymentValidateFailed",
    "TakerPaymentWaitConfirmFailed",
    "TakerPaymentSpendFailed",
    "TakerPaymentSpendConfirmFailed",
    "MakerPaymentWaitRefundStarted",
    "MakerPaymentRefunded",
    "MakerPaymentRefundFailed",
];

pub const TAKER_SUCCESS_EVENTS: [&str; 10] = [
    "Started",
    "Negotiated",
    "TakerFeeSent",
    "MakerPaymentReceived",
    "MakerPaymentWaitConfirmStarted",
    "MakerPaymentValidatedAndConfirmed",
    "TakerPaymentSent",
    "TakerPaymentSpent",
    "MakerPaymentSpent",
    "Finished",
];

pub const TAKER_ERROR_EVENTS: [&str; 13] = [
    "StartFailed",
    "NegotiateFailed",
    "TakerFeeSendFailed",
    "MakerPaymentValidateFailed",
    "MakerPaymentWaitConfirmFailed",
    "TakerPaymentTransactionFailed",
    "TakerPaymentWaitConfirmFailed",
    "TakerPaymentDataSendFailed",
    "TakerPaymentWaitForSpendFailed",
    "MakerPaymentSpendFailed",
    "TakerPaymentWaitRefundStarted",
    "TakerPaymentRefunded",
    "TakerPaymentRefundFailed",
];

/// Automatically kill a wrapped process.
pub struct RaiiKill {
    pub handle: Child,
    running: bool,
}
impl RaiiKill {
    pub fn from_handle(handle: Child) -> RaiiKill { RaiiKill { handle, running: true } }
    pub fn running(&mut self) -> bool {
        if !self.running {
            return false;
        }
        match self.handle.try_wait() {
            Ok(None) => true,
            _ => {
                self.running = false;
                false
            },
        }
    }
}
impl Drop for RaiiKill {
    fn drop(&mut self) {
        // The cached `running` check might provide some protection against killing a wrong process under the same PID,
        // especially if the cached `running` check is also used to monitor the status of the process.
        if self.running() {
            let _ = self.handle.kill();
        }
    }
}

/// When `drop`ped, dumps the given file to the stdout.
///
/// Used in the tests, copying the MM log to the test output.
///
/// Note that because of https://github.com/rust-lang/rust/issues/42474 it's currently impossible to share the MM log interactively,
/// hence we're doing it in the `drop`.
pub struct RaiiDump {
    #[cfg(not(target_arch = "wasm32"))]
    pub log_path: PathBuf,
}
#[cfg(not(target_arch = "wasm32"))]
impl Drop for RaiiDump {
    fn drop(&mut self) {
        // `term` bypasses the stdout capturing, we should only use it if the capturing was disabled.
        let nocapture = env::args().any(|a| a == "--nocapture");

        let log = slurp(&self.log_path).unwrap();

        // Make sure the log is Unicode.
        // We'll get the "io error when listing tests: Custom { kind: InvalidData, error: StringError("text was not valid unicode") }" otherwise.
        let log = String::from_utf8_lossy(&log);
        let log = log.trim();

        if let (true, true, Some(mut t)) = (nocapture, *ISATTY, term::stdout()) {
            let _ = t.fg(term::color::BRIGHT_YELLOW);
            let _ = t.write(format!("vvv {:?} vvv\n", self.log_path).as_bytes());
            let _ = t.fg(term::color::YELLOW);
            let _ = t.write(log.as_bytes());
            let _ = t.write(b"\n");
            let _ = t.reset();
        } else {
            log! ({"vvv {:?} vvv\n{}", self.log_path, log});
        }
    }
}

lazy_static! {
    /// A singleton with the IPs used by the MarketMakerIt instances created in this session.
    /// The value is set to `false` when the instance is retired.
    static ref MM_IPS: Mutex<HashMap<IpAddr, bool>> = Mutex::new (HashMap::new());
}

#[cfg(not(target_arch = "wasm32"))]
pub type LocalStart = fn(PathBuf, PathBuf, Json);

#[cfg(target_arch = "wasm32")]
pub type LocalStart = fn(MmArc);

/// An instance of a MarketMaker process started by and for an integration test.
/// Given that [in CI] the tests are executed before the build, the binary of that process is the tests binary.
#[cfg(not(target_arch = "wasm32"))]
pub struct MarketMakerIt {
    /// The MarketMaker's current folder where it will try to open the database files.
    pub folder: PathBuf,
    /// Unique (to run multiple instances) IP, like "127.0.0.$x".
    pub ip: IpAddr,
    /// The file we redirected the standard output and error streams to.
    pub log_path: PathBuf,
    /// The PID of the MarketMaker process.
    pub pc: Option<RaiiKill>,
    /// RPC API key.
    pub userpass: String,
}

/// A MarketMaker instance started by and for an integration test.
#[cfg(target_arch = "wasm32")]
pub struct MarketMakerIt {
    pub ctx: super::mm_ctx::MmArc,
    /// RPC API key.
    pub userpass: String,
}

#[cfg(not(target_arch = "wasm32"))]
impl std::fmt::Debug for MarketMakerIt {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "MarketMakerIt {{ folder: {:?}, ip: {}, log_path: {:?}, userpass: {} }}",
            self.folder, self.ip, self.log_path, self.userpass
        )
    }
}

impl MarketMakerIt {
    /// Start a new MarketMaker node without any specific environment variables.
    /// For more information see [`MarketMakerIt::start_with_envs`].
    #[cfg(not(target_arch = "wasm32"))]
    pub fn start(conf: Json, userpass: String, local: Option<LocalStart>) -> Result<MarketMakerIt, String> {
        block_on(MarketMakerIt::start_with_envs(conf, userpass, local, &[]))
    }

    /// Start a new MarketMaker node asynchronously without any specific environment variables.
    /// For more information see [`MarketMakerIt::start_with_envs`].
    pub async fn start_async(conf: Json, userpass: String, local: Option<LocalStart>) -> Result<MarketMakerIt, String> {
        MarketMakerIt::start_with_envs(conf, userpass, local, &[]).await
    }

    /// Create a new temporary directory and start a new MarketMaker process there.
    ///
    /// * `conf` - The command-line configuration passed to the MarketMaker.
    ///            Unique local IP address is injected as "myipaddr" unless this field is already present.
    /// * `userpass` - RPC API key. We should probably extract it automatically from the MM log.
    /// * `local` - Function to start the MarketMaker in a local thread, instead of spawning a process.
    /// * `envs` - The enviroment variables passed to the process
    /// It's required to manually add 127.0.0.* IPs aliases on Mac to make it properly work.
    /// cf. https://superuser.com/a/458877, https://superuser.com/a/635327
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn start_with_envs(
        mut conf: Json,
        userpass: String,
        local: Option<LocalStart>,
        envs: &[(&str, &str)],
    ) -> Result<MarketMakerIt, String> {
        let ip = try_s!(Self::myipaddr_from_conf(&mut conf));
        let folder = new_mm2_temp_folder_path(Some(ip));
        let db_dir = match conf["dbdir"].as_str() {
            Some(path) => path.into(),
            None => {
                let dir = folder.join("DB");
                conf["dbdir"] = dir.to_str().unwrap().into();
                dir
            },
        };

        try_s!(fs::create_dir(&folder));
        match fs::create_dir(db_dir) {
            Ok(_) => (),
            Err(ref ie) if ie.kind() == std::io::ErrorKind::AlreadyExists => (),
            Err(e) => return ERR!("{}", e),
        };
        let log_path = match conf["log"].as_str() {
            Some(path) => path.into(),
            None => {
                let path = folder.join("mm2.log");
                conf["log"] = path.to_str().unwrap().into();
                path
            },
        };

        // If `local` is provided
        // then instead of spawning a process we start the MarketMaker in a local thread,
        // allowing us to easily *debug* the tested MarketMaker code.
        // Note that this should only be used while running a single test,
        // using this option while running multiple tests (or multiple MarketMaker instances) is currently UB.
        let pc = if let Some(local) = local {
            local(folder.clone(), log_path.clone(), conf.clone());
            None
        } else {
            let executable = try_s!(env::args().next().ok_or("No program name"));
            let executable = try_s!(Path::new(&executable).canonicalize());
            let log = try_s!(fs::File::create(&log_path));
            let child = try_s!(Command::new(&executable)
                .arg("test_mm_start")
                .arg("--nocapture")
                .current_dir(&folder)
                .env("_MM2_TEST_CONF", try_s!(json::to_string(&conf)))
                .env("MM2_UNBUFFERED_OUTPUT", "1")
                .env("RUST_LOG", "debug")
                .envs(envs.to_vec())
                .stdout(try_s!(log.try_clone()))
                .stderr(log)
                .spawn());
            Some(RaiiKill::from_handle(child))
        };

        let mut mm = MarketMakerIt {
            folder,
            ip,
            log_path,
            pc,
            userpass,
        };

        try_s!(mm.startup_checks(&conf).await);
        Ok(mm)
    }

    /// Start a new MarketMaker locally.
    ///
    /// * `conf` - The command-line configuration passed to the MarketMaker.
    ///            Unique P2P in-memory port is injected as `p2p_in_memory_port` unless this field is already present.
    /// * `userpass` - RPC API key. We should probably extract it automatically from the MM log.
    /// * `local` - Function to start the MarketMaker locally. Required for nodes running in a browser.
    /// * `envs` - The enviroment variables passed to the process.
    ///            The argument is ignore for nodes running in a browser.
    #[cfg(target_arch = "wasm32")]
    pub async fn start_with_envs(
        mut conf: Json,
        userpass: String,
        local: Option<LocalStart>,
        _envs: &[(&str, &str)],
    ) -> Result<MarketMakerIt, String> {
        if conf["p2p_in_memory"].is_null() {
            conf["p2p_in_memory"] = Json::Bool(true);
        }

        let i_am_seed = conf["i_am_seed"].as_bool().unwrap_or_default();
        let p2p_in_memory_port_missed = conf["p2p_in_memory_port"].is_null();
        if i_am_seed && p2p_in_memory_port_missed {
            let mut rng = super::small_rng();
            let new_p2p_port: u64 = rng.gen();

            log!("Set 'p2p_in_memory_port' to "[new_p2p_port]);
            conf["p2p_in_memory_port"] = Json::Number(new_p2p_port.into());
        }

        let ctx = crate::mm_ctx::MmCtxBuilder::new()
            .with_conf(conf.clone())
            .with_test_db_namespace()
            .into_mm_arc();
        let local = try_s!(local.ok_or("!local"));
        local(ctx.clone());

        let mut mm = MarketMakerIt { ctx, userpass };
        try_s!(mm.startup_checks(&conf).await);
        Ok(mm)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn log_as_utf8(&self) -> Result<String, String> {
        let mm_log = try_s!(slurp(&self.log_path));
        let mm_log = unsafe { String::from_utf8_unchecked(mm_log) };
        Ok(mm_log)
    }

    /// Busy-wait on the log until the `pred` returns `true` or `timeout_sec` expires.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn wait_for_log<F>(&mut self, timeout_sec: f64, pred: F) -> Result<(), String>
    where
        F: Fn(&str) -> bool,
    {
        let start = now_float();
        let ms = 50.min((timeout_sec * 1000.) as u64 / 20 + 10);
        loop {
            let mm_log = try_s!(self.log_as_utf8());
            if pred(&mm_log) {
                return Ok(());
            }
            if now_float() - start > timeout_sec {
                return ERR!("Timeout expired waiting for a log condition");
            }
            if let Some(ref mut pc) = self.pc {
                if !pc.running() {
                    return ERR!("MM process terminated prematurely at: {:?}.", self.folder);
                }
            }
            Timer::sleep(ms as f64 / 1000.).await
        }
    }

    /// Busy-wait on the log until the `pred` returns `true` or `timeout_sec` expires.
    /// The difference from standard wait_for_log is this function keeps working
    /// after process is stopped
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn wait_for_log_after_stop<F>(&mut self, timeout_sec: f64, pred: F) -> Result<(), String>
    where
        F: Fn(&str) -> bool,
    {
        let start = now_float();
        let ms = 50.min((timeout_sec * 1000.) as u64 / 20 + 10);
        loop {
            let mm_log = try_s!(self.log_as_utf8());
            if pred(&mm_log) {
                return Ok(());
            }
            if now_float() - start > timeout_sec {
                return ERR!("Timeout expired waiting for a log condition");
            }
            Timer::sleep(ms as f64 / 1000.).await
        }
    }

    /// Busy-wait on the instance in-memory log until the `pred` returns `true` or `timeout_sec` expires.
    #[cfg(target_arch = "wasm32")]
    pub async fn wait_for_log<F>(&mut self, timeout_sec: f64, pred: F) -> Result<(), String>
    where
        F: Fn(&str) -> bool,
    {
        wait_for_log(&self.ctx, timeout_sec, pred).await
    }

    /// Invokes the locally running MM and returns its reply.
    #[cfg(target_arch = "wasm32")]
    pub async fn rpc(&self, payload: Json) -> Result<(StatusCode, String, HeaderMap), String> {
        let wasm_rpc = self
            .ctx
            .wasm_rpc
            .as_option()
            .expect("'MmCtx::rpc' must be initialized already");
        match wasm_rpc.request(payload).await {
            // Please note a new type of error will be introduced soon.
            Ok(body) => {
                let status_code = if body["error"].is_null() {
                    StatusCode::OK
                } else {
                    StatusCode::INTERNAL_SERVER_ERROR
                };
                let body_str = json::to_string(&body).expect(&format!("Response {:?} is not a valid JSON", body));
                Ok((status_code, body_str, HeaderMap::new()))
            },
            Err(e) => Ok((StatusCode::INTERNAL_SERVER_ERROR, e, HeaderMap::new())),
        }
    }

    /// Invokes the locally running MM and returns its reply.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn rpc(&self, payload: Json) -> Result<(StatusCode, String, HeaderMap), String> {
        let uri = format!("http://{}:7783", self.ip);
        log!("sending rpc request " (json::to_string(&payload).unwrap()) " to " (uri));

        let payload = try_s!(json::to_vec(&payload));
        let request = try_s!(Request::builder().method("POST").uri(uri).body(payload));

        let (status, headers, body) = try_s!(slurp_req(request).await);
        Ok((status, try_s!(std::str::from_utf8(&body)).trim().into(), headers))
    }

    /// Sends the &str payload to the locally running MM and returns it's reply.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn rpc_str(&self, payload: &'static str) -> Result<(StatusCode, String, HeaderMap), String> {
        let uri = format!("http://{}:7783", self.ip);
        let request = try_s!(Request::builder().method("POST").uri(uri).body(payload.into()));
        let (status, headers, body) = try_s!(block_on(slurp_req(request)));
        Ok((status, try_s!(std::str::from_utf8(&body)).trim().into(), headers))
    }

    #[cfg(target_arch = "wasm32")]
    pub fn rpc_str(&self, _payload: &'static str) -> Result<(StatusCode, String, HeaderMap), String> {
        unimplemented!()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn mm_dump(&self) -> (RaiiDump, RaiiDump) { mm_dump(&self.log_path) }

    #[cfg(target_arch = "wasm32")]
    pub fn mm_dump(&self) -> (RaiiDump, RaiiDump) { (RaiiDump {}, RaiiDump {}) }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn my_seed_addr(&self) -> String { format!("{}", self.ip) }

    /// # Panic
    ///
    /// Panic if this instance is not a seed.
    #[cfg(target_arch = "wasm32")]
    pub fn my_seed_addr(&self) -> String {
        let p2p_port = self
            .ctx
            .p2p_in_memory_port()
            .expect("This instance is not a seed, so 'p2p_in_memory_port' is None");
        format!("/memory/{}", p2p_port)
    }

    /// Send the "stop" request to the locally running MM.
    pub async fn stop(&self) -> Result<(), String> {
        let (status, body, _headers) = match self.rpc(json! ({"userpass": self.userpass, "method": "stop"})).await {
            Ok(t) => t,
            Err(err) => {
                // Downgrade the known errors into log warnings,
                // in order not to spam the unit test logs with confusing panics, obscuring the real issue.
                if err.contains("An existing connection was forcibly closed by the remote host") {
                    log!("stop] MM already down? "(err));
                    return Ok(());
                } else {
                    return ERR!("{}", err);
                }
            },
        };
        if status != StatusCode::OK {
            return ERR!("MM didn't accept a stop. body: {}", body);
        }
        Ok(())
    }

    /// Currently, we cannot wait for the `Completed IAmrelay handling for peer` log entry on WASM node,
    /// because the P2P module logs to a global logger and doesn't log to the dashboard.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn check_seednodes(&mut self) -> Result<(), String> {
        // wait for at least 1 node to be added to relay mesh
        self.wait_for_log(22., |log| log.contains("Completed IAmrelay handling for peer"))
            .await
            .map_err(|e| ERRL!("{}", e))
    }

    /// Wait for the node to start listening to new P2P connections.
    /// Please note the node is expected to be a seed.
    ///
    /// Currently, we cannot wait for the `INFO Listening on` log entry on WASM node,
    /// because the P2P module logs to a global logger and doesn't log to the dashboard.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn wait_for_p2p_listen(&mut self) -> Result<(), String> {
        self.wait_for_log(22., |log| log.contains("INFO Listening on"))
            .await
            .map_err(|e| ERRL!("{}", e))
    }

    /// Wait for the RPC to be up.
    pub async fn wait_for_rpc_is_up(&mut self) -> Result<(), String> {
        self.wait_for_log(22., |log| log.contains(">>>>>>>>> DEX stats "))
            .await
            .map_err(|e| ERRL!("{}", e))
    }

    async fn startup_checks(&mut self, conf: &Json) -> Result<(), String> {
        let skip_startup_checks = conf["skip_startup_checks"].as_bool().unwrap_or_default();
        if skip_startup_checks {
            return Ok(());
        }

        try_s!(self.wait_for_rpc_is_up().await);

        #[cfg(not(target_arch = "wasm32"))]
        {
            let is_seed = conf["i_am_seed"].as_bool().unwrap_or_default();
            if is_seed {
                try_s!(self.wait_for_p2p_listen().await);
            }

            let skip_seednodes_check = conf["skip_seednodes_check"].as_bool().unwrap_or_default();
            if conf["seednodes"].as_array().is_some() && !skip_seednodes_check {
                try_s!(self.check_seednodes().await);
            }
        }

        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn myipaddr_from_conf(conf: &mut Json) -> Result<IpAddr, String> {
        if conf["myipaddr"].is_null() {
            // Generate an unique IP.
            let mut attempts = 0;
            let mut rng = super::small_rng();
            loop {
                if attempts > 128 {
                    return ERR!("Out of local IPs?");
                }
                let ip4 = Ipv4Addr::new(127, 0, 0, rng.gen_range(1, 255));
                let ip = IpAddr::from(ip4);
                let mut mm_ips = try_s!(MM_IPS.lock());
                if mm_ips.contains_key(&ip) {
                    attempts += 1;
                    continue;
                }
                mm_ips.insert(ip, true);
                conf["myipaddr"] = format!("{}", ip).into();
                conf["rpcip"] = format!("{}", ip).into();
                return Ok(ip);
            }
        }

        // Just use the IP given in the `conf`.
        let ip: IpAddr = try_s!(try_s!(conf["myipaddr"].as_str().ok_or("myipaddr is not a string")).parse());
        let mut mm_ips = try_s!(MM_IPS.lock());
        if mm_ips.contains_key(&ip) {
            log! ({"MarketMakerIt] Warning, IP {} was already used.", ip})
        }
        mm_ips.insert(ip, true);
        Ok(ip)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for MarketMakerIt {
    fn drop(&mut self) {
        if let Ok(mut mm_ips) = MM_IPS.lock() {
            // The IP addresses might still be used by the libtorrent even after a context is dropped,
            // hence we're not trying to reuse them but rather just mark them as fried.
            if let Some(active) = mm_ips.get_mut(&self.ip) {
                *active = false
            }
        } else {
            log!("MarketMakerIt] Can't lock MM_IPS.")
        }
    }
}

#[macro_export]
macro_rules! wait_log_re {
    ($mm_it: expr, $timeout_sec: expr, $re_pred: expr) => {{
        log! ("Waiting for “" ($re_pred) "”…");
        let re = regex::Regex::new($re_pred).unwrap();
        let rc = $mm_it.wait_for_log ($timeout_sec, |line| re.is_match (line)) .await;
        if let Err (err) = rc {panic! ("{}: {}", $re_pred, err)}
    }};
}

/// Busy-wait on the log until the `pred` returns `true` or `timeout_sec` expires.
pub async fn wait_for_log<F>(ctx: &MmArc, timeout_sec: f64, pred: F) -> Result<(), String>
where
    F: Fn(&str) -> bool,
{
    let start = now_float();
    let ms = 50.min((timeout_sec * 1000.) as u32 / 20 + 10);
    let mut buf = String::with_capacity(128);
    let mut found = false;
    loop {
        ctx.log.with_tail(&mut |tail| {
            for en in tail {
                if en.format(&mut buf).is_ok() && pred(&buf) {
                    found = true;
                    break;
                }
            }
        });
        if found {
            return Ok(());
        }

        ctx.log.with_gravity_tail(&mut |tail| {
            for chunk in tail {
                if pred(chunk) {
                    found = true;
                    break;
                }
            }
        });
        if found {
            return Ok(());
        }

        if now_float() - start > timeout_sec {
            return ERR!("Timeout expired waiting for a log condition");
        }
        Timer::sleep_ms(ms).await;
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct ToWaitForLogRe {
    ctx: u32,
    timeout_sec: f64,
    re_pred: String,
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn common_wait_for_log_re(req: Bytes) -> Result<Vec<u8>, String> {
    let args: ToWaitForLogRe = try_s!(json::from_slice(&req));
    let ctx = try_s!(MmArc::from_ffi_handle(args.ctx));
    let re = try_s!(Regex::new(&args.re_pred));

    // Run the blocking `wait_for_log` in the `POOL`.
    let (tx, rx) = oneshot::channel();
    try_s!(try_s!(POOL.lock()).spawn(async move {
        let res = wait_for_log(&ctx, args.timeout_sec, |line| re.is_match(line)).await;
        let _ = tx.send(res);
    }));
    try_s!(try_s!(rx.await));

    Ok(Vec::new())
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn wait_for_log_re(ctx: &MmArc, timeout_sec: f64, re_pred: &str) -> Result<(), String> {
    let re = try_s!(Regex::new(re_pred));
    wait_for_log(&ctx, timeout_sec, |line| re.is_match(line)).await
}

#[cfg(target_arch = "wasm32")]
pub async fn wait_for_log_re(ctx: &MmArc, timeout_sec: f64, re_pred: &str) -> Result<(), String> {
    try_s!(
        helperᶜ(
            "common_wait_for_log_re",
            try_s!(json::to_vec(&ToWaitForLogRe {
                ctx: try_s!(ctx.ffi_handle()),
                timeout_sec,
                re_pred: re_pred.into()
            }))
        )
        .await
    );
    Ok(())
}

/// Create RAII variables to the effect of dumping the log and the status dashboard at the end of the scope.
#[cfg(not(target_arch = "wasm32"))]
pub fn mm_dump(log_path: &Path) -> (RaiiDump, RaiiDump) {
    (
        RaiiDump {
            log_path: log_path.to_path_buf(),
        },
        RaiiDump {
            log_path: dashboard_path(log_path).unwrap(),
        },
    )
}

/// A typical MM instance.
#[cfg(not(target_arch = "wasm32"))]
pub fn mm_spat(
    local_start: LocalStart,
    conf_mod: &dyn Fn(Json) -> Json,
) -> (&'static str, MarketMakerIt, RaiiDump, RaiiDump) {
    let passphrase = "SPATsRps3dhEtXwtnpRCKF";
    let mm = MarketMakerIt::start(
        conf_mod(json! ({
            "gui": "nogui",
            "passphrase": passphrase,
            "rpccors": "http://localhost:4000",
            "coins": [
                {"coin":"RICK","asset":"RICK","rpcport":8923},
                {"coin":"MORTY","asset":"MORTY","rpcport":11608},
            ],
            "i_am_seed": true,
            "rpc_password": "pass",
        })),
        "pass".into(),
        match super::var("LOCAL_THREAD_MM") {
            Ok(ref e) if e == "1" => Some(local_start),
            _ => None,
        },
    )
    .unwrap();
    let (dump_log, dump_dashboard) = mm_dump(&mm.log_path);
    (passphrase, mm, dump_log, dump_dashboard)
}

#[cfg(target_arch = "wasm32")]
pub fn mm_spat(
    _local_start: LocalStart,
    _conf_mod: &dyn Fn(Json) -> Json,
) -> (&'static str, MarketMakerIt, RaiiDump, RaiiDump) {
    unimplemented!()
}

#[cfg(target_arch = "wasm32")]
pub fn register_wasm_log(level: LogLevel) {
    use crate::log::{register_callback, WasmCallback, WasmLoggerBuilder};

    register_callback(WasmCallback::console_log());
    // a log is initialized already if [`WasmLoggerBuilder::try_init`] fails
    let _ = WasmLoggerBuilder::default().level_filter(level).try_init();
}

/// Asks MM to enable the given currency in electrum mode
/// fresh list of servers at https://github.com/jl777/coins/blob/master/electrums/.
pub async fn enable_electrum(mm: &MarketMakerIt, coin: &str, tx_history: bool, urls: &[&str]) -> Json {
    let servers = urls.iter().map(|url| json!({ "url": url })).collect();
    enable_electrum_json(mm, coin, tx_history, servers).await
}

/// Asks MM to enable the given currency in electrum mode
/// fresh list of servers at https://github.com/jl777/coins/blob/master/electrums/.
pub async fn enable_electrum_json(mm: &MarketMakerIt, coin: &str, tx_history: bool, servers: Vec<Json>) -> Json {
    let electrum = mm
        .rpc(json! ({
            "userpass": mm.userpass,
            "method": "electrum",
            "coin": coin,
            "servers": servers,
            "mm2": 1,
            "tx_history": tx_history,
        }))
        .await
        .unwrap();
    assert_eq!(
        electrum.0,
        StatusCode::OK,
        "RPC «electrum» failed with {} {}",
        electrum.0,
        electrum.1
    );
    json::from_str(&electrum.1).unwrap()
}

pub async fn enable_qrc20(mm: &MarketMakerIt, coin: &str, urls: &[&str], swap_contract_address: &str) -> Json {
    let servers: Vec<_> = urls.iter().map(|url| json!({ "url": url })).collect();
    let electrum = mm
        .rpc(json! ({
            "userpass": mm.userpass,
            "method": "electrum",
            "coin": coin,
            "servers": servers,
            "mm2": 1,
            "swap_contract_address": swap_contract_address,
        }))
        .await
        .unwrap();
    assert_eq!(
        electrum.0,
        StatusCode::OK,
        "RPC «electrum» failed with {} {}",
        electrum.0,
        electrum.1
    );
    json::from_str(&electrum.1).unwrap()
}

/// Reads passphrase and userpass from .env file
pub fn from_env_file(env: Vec<u8>) -> (Option<String>, Option<String>) {
    use regex::bytes::Regex;
    let (mut passphrase, mut userpass) = (None, None);
    for cap in Regex::new(r"(?m)^(PASSPHRASE|USERPASS)=(\w[\w ]+)$")
        .unwrap()
        .captures_iter(&env)
    {
        match cap.get(1) {
            Some(name) if name.as_bytes() == b"PASSPHRASE" => {
                passphrase = cap.get(2).map(|v| String::from_utf8(v.as_bytes().into()).unwrap())
            },
            Some(name) if name.as_bytes() == b"USERPASS" => {
                userpass = cap.get(2).map(|v| String::from_utf8(v.as_bytes().into()).unwrap())
            },
            _ => (),
        }
    }
    (passphrase, userpass)
}

#[macro_export]
#[cfg(target_arch = "wasm32")]
macro_rules! get_passphrase {
    ($_env_file:literal, $env:literal) => {
        option_env!($env).ok_or_else(|| ERRL!("No such '{}' environment variable", $env))
    };
}

#[macro_export]
#[cfg(not(target_arch = "wasm32"))]
macro_rules! get_passphrase {
    ($env_file:literal, $env:literal) => {
        $crate::for_tests::get_passphrase(&$env_file, $env)
    };
}

/// Reads passphrase from file or environment.
#[cfg(not(target_arch = "wasm32"))]
pub fn get_passphrase(path: &dyn AsRef<Path>, env: &str) -> Result<String, String> {
    if let (Some(file_passphrase), _file_userpass) = from_env_file(try_s!(slurp(path))) {
        return Ok(file_passphrase);
    }

    if let Ok(v) = super::var(env) {
        Ok(v)
    } else {
        ERR!("No {} or {}", env, path.as_ref().display())
    }
}

/// Asks MM to enable the given currency in native mode.
/// Returns the RPC reply containing the corresponding wallet address.
pub async fn enable_native(mm: &MarketMakerIt, coin: &str, urls: &[&str]) -> Json {
    let native = mm
        .rpc(json! ({
            "userpass": mm.userpass,
            "method": "enable",
            "coin": coin,
            "urls": urls,
            // Dev chain swap contract address
            "swap_contract_address": "0xa09ad3cd7e96586ebd05a2607ee56b56fb2db8fd",
            "mm2": 1,
        }))
        .await
        .unwrap();
    assert_eq!(native.0, StatusCode::OK, "'enable' failed: {}", native.1);
    json::from_str(&native.1).unwrap()
}

/// Use a separate (unique) temporary folder for each MM.
/// We could also remove the old folders after some time in order not to spam the temporary folder.
/// Though we don't always want to remove them right away, allowing developers to check the files).
/// Appends IpAddr if it is pre-known
pub fn new_mm2_temp_folder_path(ip: Option<IpAddr>) -> PathBuf {
    let now = super::now_ms();
    let now = Local.timestamp((now / 1000) as i64, (now % 1000) as u32 * 1_000_000);
    let folder = match ip {
        Some(ip) => format!("mm2_{}_{}", now.format("%Y-%m-%d_%H-%M-%S-%3f"), ip),
        None => format!("mm2_{}", now.format("%Y-%m-%d_%H-%M-%S-%3f")),
    };
    super::temp_dir().join(folder)
}

pub fn find_metrics_in_json(
    metrics: MetricsJson,
    search_key: &str,
    search_labels: &[(&str, &str)],
) -> Option<MetricType> {
    metrics.metrics.into_iter().find(|metric| {
        let (key, labels) = match metric {
            MetricType::Counter { key, labels, .. } => (key, labels),
            _ => return false,
        };

        if key != search_key {
            return false;
        }

        for (s_label_key, s_label_value) in search_labels.iter() {
            let label_value = match labels.get(&(*s_label_key).to_string()) {
                Some(x) => x,
                _ => return false,
            };

            if s_label_value != label_value {
                return false;
            }
        }

        true
    })
}

/// Helper function requesting my swap status and checking it's events
pub async fn check_my_swap_status(
    mm: &MarketMakerIt,
    uuid: &str,
    expected_success_events: &[&str],
    expected_error_events: &[&str],
    maker_amount: BigDecimal,
    taker_amount: BigDecimal,
) {
    let response = mm
        .rpc(json! ({
            "userpass": mm.userpass,
            "method": "my_swap_status",
            "params": {
                "uuid": uuid,
            }
        }))
        .await
        .unwrap();
    assert!(response.0.is_success(), "!status of {}: {}", uuid, response.1);
    let status_response: Json = json::from_str(&response.1).unwrap();
    let success_events: Vec<String> = json::from_value(status_response["result"]["success_events"].clone()).unwrap();
    assert_eq!(expected_success_events, success_events.as_slice());
    let error_events: Vec<String> = json::from_value(status_response["result"]["error_events"].clone()).unwrap();
    assert_eq!(expected_error_events, error_events.as_slice());

    let events_array = status_response["result"]["events"].as_array().unwrap();
    let actual_maker_amount = json::from_value(events_array[0]["event"]["data"]["maker_amount"].clone()).unwrap();
    assert_eq!(maker_amount, actual_maker_amount);
    let actual_taker_amount = json::from_value(events_array[0]["event"]["data"]["taker_amount"].clone()).unwrap();
    assert_eq!(taker_amount, actual_taker_amount);
    let actual_events = events_array.iter().map(|item| item["event"]["type"].as_str().unwrap());
    let actual_events: Vec<&str> = actual_events.collect();
    assert_eq!(expected_success_events, actual_events.as_slice());
}

pub async fn check_stats_swap_status(
    mm: &MarketMakerIt,
    uuid: &str,
    maker_expected_events: &[&str],
    taker_expected_events: &[&str],
) {
    let response = mm
        .rpc(json! ({
            "method": "stats_swap_status",
            "params": {
                "uuid": uuid,
            }
        }))
        .await
        .unwrap();
    assert!(response.0.is_success(), "!status of {}: {}", uuid, response.1);
    let status_response: Json = json::from_str(&response.1).unwrap();
    let maker_events_array = status_response["result"]["maker"]["events"].as_array().unwrap();
    let taker_events_array = status_response["result"]["taker"]["events"].as_array().unwrap();
    let maker_actual_events = maker_events_array
        .iter()
        .map(|item| item["event"]["type"].as_str().unwrap());
    let maker_actual_events: Vec<&str> = maker_actual_events.collect();
    let taker_actual_events = taker_events_array
        .iter()
        .map(|item| item["event"]["type"].as_str().unwrap());
    let taker_actual_events: Vec<&str> = taker_actual_events.collect();
    assert_eq!(maker_expected_events, maker_actual_events.as_slice());
    assert_eq!(taker_expected_events, taker_actual_events.as_slice());
}

pub async fn check_recent_swaps(mm: &MarketMakerIt, expected_len: usize) {
    let response = mm
        .rpc(json! ({
            "method": "my_recent_swaps",
            "userpass": mm.userpass,
        }))
        .await
        .unwrap();
    assert!(response.0.is_success(), "!status of my_recent_swaps {}", response.1);
    let swaps_response: Json = json::from_str(&response.1).unwrap();
    let swaps: &Vec<Json> = swaps_response["result"]["swaps"].as_array().unwrap();
    assert_eq!(expected_len, swaps.len());
}
