//! A common dependency for subcrates.
//!
//!                   common
//!                     ^
//!                     |
//!     subcrate A   ---+---   subcrate B
//!         ^                      ^
//!         |                      |
//!         +-----------+----------+
//!                     |
//!                   binary

#![allow(uncommon_codepoints)]
#![feature(non_ascii_idents, integer_atomics, panic_info_message)]
#![feature(async_closure)]
#![feature(hash_raw_entry)]
#![feature(negative_impls)]
#![feature(auto_traits)]
#![feature(drain_filter)]
#![feature(const_fn)]

#[macro_use] extern crate arrayref;
#[macro_use] extern crate fomat_macros;
#[macro_use] extern crate gstuff;
#[macro_use] extern crate lazy_static;
#[macro_use] pub extern crate serde_derive;
#[macro_use] pub extern crate serde_json;
#[cfg(test)]
#[macro_use]
extern crate ser_error_derive;

/// Fills a C character array with a zero-terminated C string,
/// returning an error if the string is too large.
#[macro_export]
#[allow(unused_unsafe)]
macro_rules! safecopy {
    ($to: expr, $format: expr, $($args: tt)+) => {{
        use ::std::io::Write;
        let to: &mut [i8] = &mut $to[..];  // Check the type.
        let to: &mut [u8] = unsafe {::std::mem::transmute (to)};  // c_char to Rust.
        let mut wr = ::std::io::Cursor::new (to);
        write! (&mut wr, concat! ($format, "\0"), $($args)+)
    }}
}

/// Implements a `From` for `enum` with a variant name matching the name of the type stored.
///
/// This is helpful as a workaround for the lack of datasort refinements.  
/// And also as a simpler alternative to `enum_dispatch` and `enum_derive`.
///
///     enum Color {Red (Red)}
///     ifrom! (Color, Red);
#[macro_export]
macro_rules! ifrom {
    ($enum: ident, $id: ident) => {
        impl From<$id> for $enum {
            fn from(t: $id) -> $enum { $enum::$id(t) }
        }
    };
}

#[macro_export]
macro_rules! cfg_wasm32 {
    ($($tokens:tt)*) => {
        cfg_if::cfg_if! {
            if #[cfg(target_arch = "wasm32")] {
                $($tokens)*
            }
        }
    };
}

#[macro_export]
macro_rules! cfg_native {
    ($($tokens:tt)*) => {
        cfg_if::cfg_if! {
            if #[cfg(not(target_arch = "wasm32"))] {
                $($tokens)*
            }
        }
    };
}

#[macro_use]
pub mod jsonrpc_client;
#[macro_use]
pub mod log;
#[macro_use]
pub mod mm_metrics;

pub mod big_int_str;
pub mod crash_reports;
pub mod custom_futures;
pub mod duplex_mutex;
pub mod file_lock;
#[cfg(not(target_arch = "wasm32"))] pub mod for_c;
pub mod iguana_utils;
pub mod mm_ctx;
#[path = "mm_error/mm_error.rs"] pub mod mm_error;
pub mod mm_number;
pub mod privkey;
pub mod seri;
#[path = "patterns/state_machine.rs"] pub mod state_machine;

#[cfg(target_arch = "wasm32")]
#[path = "indexed_db/indexed_db.rs"]
pub mod indexed_db;
#[cfg(target_arch = "wasm32")] pub mod wasm_rpc;
#[cfg(target_arch = "wasm32")]
#[path = "transport/wasm_ws.rs"]
pub mod wasm_ws;

use atomic::Atomic;
use bigdecimal::BigDecimal;
use futures::compat::Future01CompatExt;
use futures::future::FutureExt;
use futures::task::Waker;
use futures01::{future, task::Task, Future};
use gstuff::binprint;
use hex::FromHex;
use http::header::{HeaderValue, CONTENT_TYPE};
use http::{HeaderMap, Request, Response, StatusCode};
use parking_lot::{Mutex as PaMutex, MutexGuard as PaMutexGuard};
use rand::{rngs::SmallRng, SeedableRng};
use serde::{de, ser};
use serde_bytes::ByteBuf;
use serde_json::{self as json, Value as Json};
use std::collections::HashMap;
use std::ffi::{CStr, OsStr};
use std::fmt::{self, Write as FmtWrite};
use std::fs;
use std::fs::DirEntry;
use std::future::Future as Future03;
use std::io::Write;
use std::mem::{forget, size_of, zeroed};
use std::net::SocketAddr;
use std::ops::{Add, Deref, Div, RangeInclusive};
use std::os::raw::{c_char, c_void};
use std::panic::{set_hook, PanicInfo};
use std::path::{Path, PathBuf};
use std::ptr::read_volatile;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

pub use serde;

cfg_native! {
    pub use gstuff::{now_float, now_ms};
    pub use rusqlite;

    #[cfg(not(windows))]
    use findshlibs::{IterationControl, Segment, SharedLibrary, TargetSharedLibrary};
    use libc::{free, malloc};
    use std::env;
    use std::io::Read;
}

cfg_wasm32! {
    use futures::task::{Context, Poll as Poll03};
    use std::pin::Pin;
    use wasm_bindgen::prelude::*;
}

pub const SATOSHIS: u64 = 100_000_000;

pub const DEX_FEE_ADDR_PUBKEY: &str = "03bc2c7ba671bae4a6fc835244c9762b41647b9827d4780a89a949b984a8ddcc06";
lazy_static! {
    pub static ref DEX_FEE_ADDR_RAW_PUBKEY: Vec<u8> =
        hex::decode(DEX_FEE_ADDR_PUBKEY).expect("DEX_FEE_ADDR_PUBKEY is expected to be a hexadecimal string");
}

pub auto trait NotSame {}
impl<X> !NotSame for (X, X) {}

/// Converts u64 satoshis to f64
pub fn sat_to_f(sat: u64) -> f64 { sat as f64 / SATOSHIS as f64 }

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
#[repr(transparent)]
pub struct bits256 {
    pub bytes: [u8; 32],
}

impl Default for bits256 {
    fn default() -> bits256 {
        bits256 {
            bytes: unsafe { zeroed() },
        }
    }
}

impl fmt::Display for bits256 {
    fn fmt(&self, fm: &mut fmt::Formatter) -> fmt::Result {
        for &ch in self.bytes.iter() {
            fn hex_from_digit(num: u8) -> char {
                if num < 10 {
                    (b'0' + num) as char
                } else {
                    (b'a' + num - 10) as char
                }
            }
            fm.write_char(hex_from_digit(ch / 16))?;
            fm.write_char(hex_from_digit(ch % 16))?;
        }
        Ok(())
    }
}

impl ser::Serialize for bits256 {
    fn serialize<S>(&self, se: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        se.serialize_bytes(&self.bytes[..])
    }
}

impl<'de> de::Deserialize<'de> for bits256 {
    fn deserialize<D>(deserializer: D) -> Result<bits256, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct Bits256Visitor;
        impl<'de> de::Visitor<'de> for Bits256Visitor {
            type Value = bits256;
            fn expecting(&self, fm: &mut fmt::Formatter) -> fmt::Result { fm.write_str("a byte array") }
            fn visit_seq<S>(self, mut seq: S) -> Result<bits256, S::Error>
            where
                S: de::SeqAccess<'de>,
            {
                let mut bytes: [u8; 32] = [0; 32];
                let mut pos = 0;
                while let Some(byte) = seq.next_element()? {
                    if pos >= bytes.len() {
                        return Err(de::Error::custom("bytes length > 32"));
                    }
                    bytes[pos] = byte;
                    pos += 1;
                }
                Ok(bits256 { bytes })
            }
            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if v.len() != 32 {
                    return Err(de::Error::custom("bytes length <> 32"));
                }
                Ok(bits256 {
                    bytes: *array_ref![v, 0, 32],
                })
            }
        }
        deserializer.deserialize_bytes(Bits256Visitor)
    }
}

impl fmt::Debug for bits256 {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result { (self as &dyn fmt::Display).fmt(f) }
}

impl From<[u8; 32]> for bits256 {
    fn from(bytes: [u8; 32]) -> Self { bits256 { bytes } }
}

impl bits256 {
    /// Returns true if the hash is not zero.  
    /// Port of `#define bits256_nonz`.
    pub fn nonz(&self) -> bool { self.bytes.iter().any(|ch| *ch != 0) }
}

pub fn nonz(k: [u8; 32]) -> bool { k.iter().any(|ch| *ch != 0) }

/// Decodes a HEX string into a 32-bytes array.  
/// But only if the HEX string is 64 characters long, returning a zeroed array otherwise.  
/// (Use `fn nonz` to check if the array is zeroed).  
/// A port of cJSON.c/jbits256.
pub fn jbits256(json: &Json) -> Result<bits256, String> {
    if let Some(hex) = json.as_str() {
        if hex.len() == 64 {
            //try_s! (::common::iguana_utils::decode_hex (unsafe {&mut hash.bytes[..]}, hex.as_bytes()));
            let bytes: [u8; 32] = try_s!(FromHex::from_hex(hex));
            return Ok(bits256::from(bytes));
        }
    }
    Ok(unsafe { zeroed() })
}

pub const SATOSHIDEN: i64 = 100_000_000;
pub fn dstr(x: i64, decimals: u8) -> f64 { x as f64 / 10.0_f64.powf(decimals as f64) }

/// Apparently helps to workaround `double` fluctuations occuring on *certain* systems.
/// cf. https://stackoverflow.com/questions/19804472/double-randomly-adds-0-000000000000001.
/// Not sure it's needed in Rust, the floating point operations should be determenistic here,
/// but better safe than sorry.
pub const SMALLVAL: f64 = 0.000_000_000_000_001; // 1e-15f64

/// Helps sharing a string slice with C code by allocating a zero-terminated string with the C standard library allocator.
///
/// The difference from `CString` is that the memory is then *owned* by the C code instead of being temporarily borrowed,
/// that is it doesn't need to be recycled in Rust.
/// Plus we don't check the slice for zeroes, most of our code doesn't need that extra check.
#[cfg(not(target_arch = "wasm32"))]
pub fn str_to_malloc(s: &str) -> *mut c_char { slice_to_malloc(s.as_bytes()) as *mut c_char }

/// Helps sharing a byte slice with C code by allocating a zero-terminated string with the C standard library allocator.
#[cfg(not(target_arch = "wasm32"))]
pub fn slice_to_malloc(bytes: &[u8]) -> *mut u8 {
    unsafe {
        let buf = malloc(bytes.len() + 1) as *mut u8;
        std::intrinsics::copy(bytes.as_ptr(), buf, bytes.len());
        *buf.add(bytes.len()) = 0;
        buf
    }
}

/// Converts *mut c_char to Rust String
/// Doesn't free the allocated memory
/// It's responsibility of the caller to free the memory when required
/// Returns error in case of null pointer input
#[allow(clippy::missing_safety_doc)]
pub unsafe fn c_char_to_string(ptr: *mut c_char) -> Result<String, String> {
    if !ptr.is_null() {
        let res_str = try_s!(CStr::from_ptr(ptr).to_str());
        let res_str = String::from(res_str);
        Ok(res_str)
    } else {
        ERR!("Tried to convert null pointer to Rust String!")
    }
}

/// Frees C raw pointer
/// Does nothing in case of null pointer input
#[cfg(not(target_arch = "wasm32"))]
pub fn free_c_ptr(ptr: *mut c_void) {
    unsafe {
        if !ptr.is_null() {
            free(ptr as *mut libc::c_void);
        }
    }
}

/// Use the value, preventing the compiler and linker from optimizing it away.
pub fn black_box<T>(v: T) -> T {
    // https://github.com/rust-lang/rfcs/issues/1484#issuecomment-240853111
    //std::hint::black_box (v)

    let ret = unsafe { read_volatile(&v) };
    forget(v);
    ret
}

/// Attempts to remove the `Path` on `drop`.
#[derive(Debug)]
pub struct RaiiRm<'a>(pub &'a Path);
impl<'a> AsRef<Path> for RaiiRm<'a> {
    fn as_ref(&self) -> &Path { self.0 }
}
impl<'a> Drop for RaiiRm<'a> {
    fn drop(&mut self) { let _ = fs::remove_file(self); }
}

/// Using a static buffer in order to minimize the chance of heap and stack allocations in the signal handler.
fn trace_buf() -> PaMutexGuard<'static, [u8; 256]> {
    static TRACE_BUF: PaMutex<[u8; 256]> = PaMutex::new([0; 256]);
    TRACE_BUF.lock()
}

fn trace_name_buf() -> PaMutexGuard<'static, [u8; 128]> {
    static TRACE_NAME_BUF: PaMutex<[u8; 128]> = PaMutex::new([0; 128]);
    TRACE_NAME_BUF.lock()
}

/// Formats a stack frame.
/// Some common and less than useful frames are skipped.
pub fn stack_trace_frame(instr_ptr: *mut c_void, buf: &mut dyn Write, symbol: &backtrace::Symbol) {
    let filename = match symbol.filename() {
        Some(path) => match path.components().rev().next() {
            Some(c) => c.as_os_str().to_string_lossy(),
            None => "??".into(),
        },
        None => "??".into(),
    };
    let lineno = symbol.lineno().unwrap_or(0);
    let name = match symbol.name() {
        Some(name) => name,
        None => SymbolName::new(&[]),
    };
    let mut name_buf = trace_name_buf();
    let name = gstring!(name_buf, {
        let _ = write!(name_buf, "{}", name); // NB: `fmt` is different from `SymbolName::as_str`.
    });

    // Skip common and less than informative frames.

    match name {
        "mm2::crash_reports::rust_seh_handler"
        | "veh_exception_filter"
        | "common::stack_trace"
        | "common::log_stacktrace"
        // Super-main on Windows.
        | "__scrt_common_main_seh" => return,
        _ => (),
    }

    match filename.as_ref() {
        "boxed.rs" | "panic.rs" => return,
        _ => (),
    }

    if name.starts_with("alloc::")
        || name.starts_with("backtrace::")
        || name.starts_with("common::set_panic_hook")
        || name.starts_with("common::stack_trace")
        || name.starts_with("core::ops::")
        || name.starts_with("futures::")
        || name.starts_with("hyper::")
        || name.starts_with("mm2::crash_reports::signal_handler")
        || name.starts_with("panic_unwind::")
        || name.starts_with("std::")
        || name.starts_with("scoped_tls::")
        || name.starts_with("test::run_test::")
        || name.starts_with("tokio::")
        || name.starts_with("tokio_core::")
        || name.starts_with("tokio_reactor::")
        || name.starts_with("tokio_executor::")
        || name.starts_with("tokio_timer::")
    {
        return;
    }

    let _ = writeln!(buf, "  {}:{}] {} {:?}", filename, lineno, name, instr_ptr);
}

/// Generates a string with the current stack trace.
///
/// To get a simple stack trace:
///
///     let mut trace = String::with_capacity (4096);
///     stack_trace (&mut stack_trace_frame, &mut |l| trace.push_str (l));
///
/// * `format` - Generates the string representation of a frame.
/// * `output` - Function used to print the stack trace.
///              Printing immediately, without buffering, should make the tracing somewhat more reliable.
pub fn stack_trace(
    format: &mut dyn FnMut(*mut c_void, &mut dyn Write, &backtrace::Symbol),
    output: &mut dyn FnMut(&str),
) {
    // cf. https://github.com/rust-lang/rust/pull/64154 (standard library backtrace)

    backtrace::trace(|frame| {
        backtrace::resolve(frame.ip(), |symbol| {
            let mut trace_buf = trace_buf();
            let trace = gstring!(trace_buf, {
                // frame.ip() is next instruction pointer typically so offset(-1) points to current instruction
                format(frame.ip().wrapping_offset(-1), trace_buf, symbol);
            });
            output(trace);
        });
        true
    });

    // not(wasm) and not(windows)
    #[cfg(not(any(target_arch = "wasm32", windows)))]
    output_pc_mem_addr(output)
}

// not(wasm) and not(windows)
#[cfg(not(any(target_arch = "wasm32", windows)))]
fn output_pc_mem_addr(output: &mut dyn FnMut(&str)) {
    TargetSharedLibrary::each(|shlib| {
        let mut trace_buf = trace_buf();
        let name = gstring!(trace_buf, {
            let _ = write!(
                trace_buf,
                "Virtual memory addresses of {}",
                shlib.name().to_string_lossy()
            );
        });
        output(name);
        for seg in shlib.segments() {
            let segment = gstring!(trace_buf, {
                let _ = write!(
                    trace_buf,
                    "  {}:{}",
                    seg.name(),
                    seg.actual_virtual_memory_address(shlib)
                );
            });
            output(segment);
        }
        // First TargetSharedLibrary is initial executable, we are not interested in other libs
        IterationControl::Break
    });
}

/// Set up a panic hook that prints the panic location, the message and the backtrace.
/// (The default Rust handler doesn't have the means to print the message).
#[cfg(target_arch = "wasm32")]
pub fn set_panic_hook() {
    set_hook(Box::new(|info: &PanicInfo| {
        let mut trace = String::new();
        stack_trace(&mut stack_trace_frame, &mut |l| trace.push_str(l));
        console_err!("{}", info);
        console_err!("backtrace\n{}", trace);
    }))
}

/// Sets our own panic handler using patched backtrace crate. It was discovered that standard Rust panic
/// handlers print only "unknown" in Android backtraces which is not helpful.
/// Using custom hook with patched backtrace version solves this issue.
/// NB: https://github.com/rust-lang/backtrace-rs/issues/227
#[cfg(not(target_arch = "wasm32"))]
pub fn set_panic_hook() {
    thread_local! {static ENTERED: Atomic<bool> = Atomic::new (false);}

    set_hook(Box::new(|info: &PanicInfo| {
        // Stack tracing and logging might panic (in `println!` for example).
        // Let us detect this and do nothing on second panic.
        // We'll likely still get a crash after the hook is finished
        // (experimenting with this I'm getting the "thread panicked while panicking. aborting." on Windows)
        // but that crash will have a better stack trace compared to the one with deep hook recursion.
        if let Ok(Err(_)) = ENTERED.try_with(|e| e.compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed))
        {
            return;
        }

        let mut trace = String::new();
        stack_trace(&mut stack_trace_frame, &mut |l| trace.push_str(l));
        log!((info));
        log!("backtrace\n"(trace));

        let _ = ENTERED.try_with(|e| e.compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed));
    }))
}

/// Simulates the panic-in-panic crash.
pub fn double_panic_crash() {
    struct Panicker;
    impl Drop for Panicker {
        fn drop(&mut self) { panic!("panic in drop") }
    }
    let panicker = Panicker;
    if 1 < 2 {
        panic!("first panic")
    }
    drop(panicker) // Delays the drop.
}

/// Tries to detect if we're running under a test, allowing us to be lazy and *delay* some costly operations.
///
/// Note that the code SHOULD behave uniformely regardless of where it's invoked from
/// (nondeterminism breaks POLA and we don't know how the code will be used in the future)
/// but in certain cases we have a leeway of adjusting to being run from a test
/// without breaking any invariants or expectations.
/// For instance, DHT might take unknown time to initialize, and by delaying this initialization in the tests
/// we can avoid the unnecessary overhead of DHT initializaion and destruction while maintaining the contract.
pub fn is_a_test_drill() -> bool {
    // Stack tracing would sometimes crash on Windows, doesn't worth the risk here.
    if cfg!(windows) {
        return false;
    }

    let mut trace = String::with_capacity(1024);
    stack_trace(
        &mut |_ptr, mut fwr, sym| {
            if let Some(name) = sym.name() {
                let _ = witeln!(fwr, (name));
            }
        },
        &mut |tr| trace.push_str(tr),
    );

    if trace.contains("\nmm2::main\n") || trace.contains("\nmm2::run_lp_main\n") {
        return false;
    }

    if let Some(executable) = std::env::args().next() {
        if executable.ends_with(r"\mm2.exe") {
            return false;
        }
        if executable.ends_with("/mm2") {
            return false;
        }
    }

    true
}

pub type SlurpRes = Result<(StatusCode, HeaderMap, Vec<u8>), String>;

/// RPC response, returned by the RPC handlers.  
/// NB: By default the future is executed on the shared asynchronous reactor (`CORE`),
/// the handler is responsible for spawning the future on another reactor if it doesn't fit the `CORE` well.
pub type HyRes = Box<dyn Future<Item = Response<Vec<u8>>, Error = String> + Send>;

pub trait HttpStatusCode {
    fn status_code(&self) -> StatusCode;
}

#[derive(Debug, Deserialize, Serialize)]
struct HostedHttpRequest {
    method: String,
    uri: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

#[derive(Debug, Deserialize, Serialize)]
struct HostedHttpResponse {
    status: u16,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

// To improve git history and ease of exploratory refactoring
// we're splitting the code in place with conditional compilation.
// wio stands for "web I/O" or "wasm I/O",
// it contains the parts which aren't directly available with WASM.

#[cfg(target_arch = "wasm32")]
pub mod wio {
    use super::SlurpRes;
    use http::header::{HeaderName, HeaderValue};
    use http::{HeaderMap, Request, StatusCode};
    use serde_bencode::de::from_bytes as bdecode;
    use serde_bencode::ser::to_bytes as bencode;
    use std::str::FromStr;

    pub async fn slurp_reqʹ(request: Request<Vec<u8>>) -> Result<(StatusCode, HeaderMap, Vec<u8>), String> {
        let (parts, body) = request.into_parts();

        let hhreq = super::HostedHttpRequest {
            method: parts.method.as_str().to_owned(),
            uri: fomat!((parts.uri)),
            headers: parts
                .headers
                .iter()
                .filter_map(|(name, value)| {
                    let name = name.as_str().to_owned();
                    let v = match value.to_str() {
                        Ok(ascii) => ascii,
                        Err(err) => {
                            log! ("!ascii '" (name) "': " (err));
                            return None;
                        },
                    };
                    Some((name, v.to_owned()))
                })
                .collect(),
            body,
        };

        let hhreq = try_s!(bencode(&hhreq));
        let hhres = try_s!(super::helperᶜ("slurp_req", hhreq).await);
        let hhres: super::HostedHttpResponse = try_s!(bdecode(&hhres));
        let status = try_s!(StatusCode::from_u16(hhres.status));

        let mut headers = HeaderMap::<HeaderValue>::with_capacity(hhres.headers.len());
        for (n, v) in hhres.headers {
            headers.insert(
                try_s!(HeaderName::from_str(&n[..])),
                try_s!(HeaderValue::from_str(&v[..])),
            );
        }

        Ok((status, headers, hhres.body))
    }

    pub async fn slurp_req(request: Request<Vec<u8>>) -> SlurpRes { slurp_reqʹ(request).await }
}

#[cfg(not(target_arch = "wasm32"))]
pub mod wio {
    use crate::SlurpRes;
    use futures::compat::Future01CompatExt;
    use futures::executor::ThreadPool;
    use futures01::sync::oneshot::{self, Receiver};
    use futures01::{Async, Future, Poll};
    use futures_cpupool::CpuPool;
    use gstuff::{duration_to_float, now_float};
    use http::{HeaderMap, Request, StatusCode};
    use hyper::client::HttpConnector;
    use hyper::{Body, Client};
    use hyper_rustls::HttpsConnector;
    use std::fmt;
    use std::sync::Mutex;
    use std::thread::JoinHandle;
    use std::time::Duration;
    use tokio::runtime::Runtime;

    fn start_core_thread() -> Mm2Runtime { Mm2Runtime(Runtime::new().unwrap()) }

    pub struct Mm2Runtime(pub Runtime);

    lazy_static! {
        /// Shared asynchronous reactor.
        pub static ref CORE: Mm2Runtime = start_core_thread();
        /// Shared CPU pool to run intensive/sleeping requests on a separate thread.
        ///
        /// Deprecated, prefer the futures 0.3 `POOL` instead.
        pub static ref CPUPOOL: CpuPool = CpuPool::new(8);
        /// Shared CPU pool to run intensive/sleeping requests on s separate thread.
        pub static ref POOL: Mutex<ThreadPool> = Mutex::new(ThreadPool::builder()
            .pool_size(8)
            .name_prefix("POOL")
            .create().expect("!ThreadPool"));
    }

    impl<Fut: std::future::Future<Output = ()> + Send + 'static> hyper::rt::Executor<Fut> for &Mm2Runtime {
        fn execute(&self, fut: Fut) { self.0.spawn(fut); }
    }

    /// With a shared reactor drives the future `f` to completion.
    ///
    /// NB: This function is only useful if you need to get the results of the execution.
    /// If the results are not necessary then a future can be scheduled directly on the reactor:
    ///
    ///     CORE.spawn (|_| f);
    pub fn drive<F, R, E>(f: F) -> Receiver<Result<R, E>>
    where
        F: Future<Item = R, Error = E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        let (sx, rx) = oneshot::channel();
        CORE.0.spawn(
            f.then(move |fr: Result<R, E>| -> Result<(), ()> {
                let _ = sx.send(fr);
                Ok(())
            })
            .compat(),
        );
        rx
    }

    pub fn drive03<F, O>(f: F) -> futures::channel::oneshot::Receiver<O>
    where
        F: std::future::Future<Output = O> + Send + 'static,
        O: Send + 'static,
    {
        let (sx, rx) = futures::channel::oneshot::channel();
        CORE.0.spawn(async move {
            let res = f.await;
            if sx.send(res).is_err() {
                log!("drive03 receiver is dropped");
            };
        });
        rx
    }

    /// With a shared reactor drives the future `f` to completion.
    ///
    /// Similar to `fn drive`, but returns a stringified error,
    /// allowing us to collapse the `Receiver` and return the `R` directly.
    pub fn drive_s<F, R, E>(f: F) -> impl Future<Item = R, Error = String>
    where
        F: Future<Item = R, Error = E> + Send + 'static,
        R: Send + 'static,
        E: fmt::Display + Send + 'static,
    {
        drive(f).then(move |r| -> Result<R, String> {
            let r = try_s!(r); // Peel the `Receiver`.
            let r = try_s!(r); // `E` to `String`.
            Ok(r)
        })
    }

    /// Finishes with the "timeout" error if the underlying future isn't ready withing the given timeframe.
    ///
    /// NB: Tokio timers (in `tokio::timer`) only seem to work under the Tokio runtime,
    /// which is unfortunate as we want the different futures executed on the different reactors
    /// depending on how much they're I/O-bound, CPU-bound or blocking.
    /// Unlike the Tokio timers this `Timeout` implementation works with any reactor.
    /// Another option to consider is https://github.com/alexcrichton/futures-timer.
    /// P.S. The older `0.1` version of the `tokio::timer` might work NP, it works in other parts of our code.
    ///      The new version, on the other hand, requires the Tokio runtime (https://tokio.rs/blog/2018-03-timers/).
    /// P.S. We could try using the `futures-timer` crate instead, but note that it is currently under-maintained,
    ///      https://github.com/rustasync/futures-timer/issues/9#issuecomment-400802515.
    pub struct Timeout<R> {
        fut: Box<dyn Future<Item = R, Error = String>>,
        started: f64,
        timeout: f64,
        monitor: Option<JoinHandle<()>>,
    }
    impl<R> Future for Timeout<R> {
        type Item = R;
        type Error = String;
        fn poll(&mut self) -> Poll<R, String> {
            match self.fut.poll() {
                Err(err) => Err(err),
                Ok(Async::Ready(r)) => Ok(Async::Ready(r)),
                Ok(Async::NotReady) => {
                    let now = now_float();
                    if now >= self.started + self.timeout {
                        Err(format!("timeout ({:.1} > {:.1})", now - self.started, self.timeout))
                    } else {
                        // Start waking up this future until it has a chance to timeout.
                        // For now it's just a basic separate thread. Will probably optimize later.
                        if self.monitor.is_none() {
                            let task = futures01::task::current();
                            let deadline = self.started + self.timeout;
                            self.monitor = Some(
                                std::thread::Builder::new()
                                    .name("timeout monitor".into())
                                    .spawn(move || loop {
                                        std::thread::sleep(Duration::from_secs(1));
                                        task.notify();
                                        if now_float() > deadline + 2. {
                                            break;
                                        }
                                    })
                                    .unwrap(),
                            );
                        }
                        Ok(Async::NotReady)
                    }
                },
            }
        }
    }
    impl<R> Timeout<R> {
        pub fn new(fut: Box<dyn Future<Item = R, Error = String>>, timeout: Duration) -> Timeout<R> {
            Timeout {
                fut,
                started: now_float(),
                timeout: duration_to_float(timeout),
                monitor: None,
            }
        }
    }

    unsafe impl<R> Send for Timeout<R> {}

    /// Initialize the crate.
    pub fn init() {
        // Pre-allocate the stack trace buffer in order to avoid allocating it from a signal handler.
        super::black_box(&*super::trace_buf());
        super::black_box(&*super::trace_name_buf());
    }

    lazy_static! {
        /// NB: With a shared client there is a possibility that keep-alive connections will be reused.
        pub static ref HYPER: Client<HttpsConnector<HttpConnector>> = {
            // Please note there was a problem on iOS if [`HttpsConnector::with_native_roots`] is used instead.
            let https = HttpsConnector::with_webpki_roots();
            Client::builder()
                .executor(&*CORE)
                // Hyper had a lot of Keep-Alive bugs over the years and I suspect
                // that with the shared client we might be getting errno 10054
                // due to a closed Keep-Alive connection mismanagement.
                // (To solve this problem Hyper should proactively close the Keep-Alive
                // connections after a configurable amount of time has passed since
                // their creation, thus saving us from trying to use the connections
                // closed on the other side. I wonder if we can implement this strategy
                // ourselves with a custom connector or something).
                // Performance of Keep-Alive in the Hyper client is questionable as well,
                // should measure it on a case-by-case basis when we need it.
                .pool_max_idle_per_host(0)
                .build(https)
        };
    }

    /// Executes a Hyper request, returning the response status, headers and body.
    pub async fn slurp_req(request: Request<Vec<u8>>) -> SlurpRes {
        let (head, body) = request.into_parts();
        let request = Request::from_parts(head, Body::from(body));

        let request_f = HYPER.request(request);
        let response = try_s!(try_s!(drive03(request_f).await));
        let status = response.status();
        let headers = response.headers().clone();
        let body = response.into_body();
        let output = try_s!(hyper::body::to_bytes(body).await);
        Ok((status, headers, output.to_vec()))
    }

    pub async fn slurp_reqʹ(request: Request<Vec<u8>>) -> Result<(StatusCode, HeaderMap, Vec<u8>), String> {
        slurp_req(request).await
    }
}

pub mod lazy {
    #[cfg(test)] use super::block_on;
    use async_trait::async_trait;
    use std::future::Future;

    /// Wrapper of lazily evaluated variables.
    /// A `LazyLocal` object initializes the [`LazyLocal::inner`] value once
    /// on [`LazyLocal::get()`] or [`LazyLocal::get_mut()`] calls using [`LazyLocal::constructor`] callback.
    pub struct LazyLocal<'a, T> {
        inner: Option<T>,
        constructor: Option<Box<dyn FnOnce() -> T + 'a>>,
    }

    impl<'a, T> LazyLocal<'a, T> {
        pub fn with_constructor(constructor: impl FnOnce() -> T + 'a) -> Self {
            Self {
                inner: None,
                constructor: Some(Box::new(constructor)),
            }
        }

        pub fn is_initialized(&self) -> bool { self.inner.is_some() }

        /// Initialize the [`LazyLocal::inner`] value if it is not yet and get the immutable reference on it.
        pub fn get(&mut self) -> &T { self.get_mut() }

        /// Initialize the [`LazyLocal::inner`] value if it is not yet and get the mutable reference on it.
        pub fn get_mut(&mut self) -> &mut T {
            match self.inner {
                Some(ref mut inner) => inner,
                None => {
                    let mut constructor = None;
                    std::mem::swap(&mut self.constructor, &mut constructor);
                    let constructor = constructor.expect("constructor is used already");
                    self.inner = Some(constructor());
                    self.inner.as_mut().unwrap()
                },
            }
        }
    }

    /// Searches for an element of an iterator that satisfies a predicate function.
    /// `find_lazy()` takes a closure that returns `true` or `false`.
    /// It applies this closure to each execution result of the futures stored within iterator, and if any of them return
    /// `true`, then `find_lazy()` returns [`Some(element)`]. If they all return
    /// `false`, it returns [`None`].
    #[async_trait]
    pub trait FindLazy: Iterator {
        type FutureOutput;

        async fn find_lazy<F>(self, f: F) -> Option<Self::FutureOutput>
        where
            F: FnMut(&Self::FutureOutput) -> bool + Send;
    }

    /// Is equivalent to `FindLazy` except a predicate function can modify a return element.
    #[async_trait]
    pub trait FindMapLazy: Iterator {
        type FutureOutput;

        async fn find_map_lazy<M, F>(self, f: F) -> Option<M>
        where
            F: FnMut(Self::FutureOutput) -> Option<M> + Send;
    }

    /// Implement the `FindLazy` for iterator of futures.
    #[async_trait]
    impl<T, I> FindLazy for I
    where
        T: Future + Send + 'static,
        I: Iterator<Item = T> + Send,
    {
        type FutureOutput = T::Output;

        async fn find_lazy<F>(mut self, mut f: F) -> Option<Self::FutureOutput>
        where
            F: FnMut(&Self::FutureOutput) -> bool + Send,
        {
            for item in self {
                let result = item.await;
                if f(&result) {
                    return Some(result);
                }
            }
            None
        }
    }

    /// Implement the `FindMapLazy` for iterators of futures.
    #[async_trait]
    impl<T, I> FindMapLazy for I
    where
        T: Future + Send + 'static,
        I: Iterator<Item = T> + Send,
    {
        type FutureOutput = T::Output;

        async fn find_map_lazy<M, F>(mut self, mut f: F) -> Option<M>
        where
            F: FnMut(Self::FutureOutput) -> Option<M> + Send,
        {
            for item in self {
                let mres = f(item.await);
                if mres.is_some() {
                    return mres;
                }
            }
            None
        }
    }

    #[cfg(test)]
    async fn future_helper(msg: &str, panic: bool) -> String {
        if panic {
            panic!("This future must not be executed");
        }

        msg.into()
    }

    #[test]
    fn test_lazy_local() {
        let template = "Default".to_string();
        let mut local = LazyLocal::with_constructor(|| template.clone());

        assert!(!local.is_initialized());
        assert!(local.constructor.is_some());
        assert_eq!(local.inner, None);

        assert_eq!(local.get(), &template);
        assert!(local.is_initialized());
        assert!(local.constructor.is_none());
        assert_eq!(local.get_mut(), &template);

        *local.get_mut() = "Another string".into();
        assert_eq!(local.get(), "Another string");
    }

    #[test]
    fn test_find_lazy() {
        let futures = vec![
            future_helper("say", false),
            future_helper("hello", false),
            future_helper("world", true), // this future must not be executed
        ];

        let actual = block_on(futures.into_iter().find_lazy(|x| x == "hello"));
        assert_eq!(actual, Some("hello".into()));
    }

    #[test]
    fn test_find_map_lazy() {
        let futures = vec![
            future_helper("say", false),
            future_helper("hello", false),
            future_helper("world", true), // this future must not be executed
        ];

        let actual =
            block_on(
                futures
                    .into_iter()
                    .find_map_lazy(|x| if x == "hello" { Some(x.to_uppercase()) } else { None }),
            );
        assert_eq!(actual, Some("HELLO".into()));
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub mod executor {
    use futures::task::Context;
    use futures::task::Poll as Poll03;
    use futures::Future as Future03;
    use gstuff::now_float;
    use std::pin::Pin;
    use std::thread;
    use std::time::Duration;

    pub fn spawn(future: impl Future03<Output = ()> + Send + 'static) { crate::wio::CORE.0.spawn(future); }

    pub fn spawn_boxed(future: Box<dyn Future03<Output = ()> + Send + Unpin + 'static>) { spawn(future); }

    /// Schedule the given `future` to be executed shortly after the given `utc` time is reached.
    pub fn spawn_after(utc: f64, future: impl Future03<Output = ()> + Send + 'static) {
        use crossbeam::channel;
        use gstuff::Constructible;
        use std::collections::BTreeMap;
        use std::sync::Once;

        type SheduleChannelItem = (f64, Pin<Box<dyn Future03<Output = ()> + Send + 'static>>);
        static START: Once = Once::new();
        static SCHEDULE: Constructible<channel::Sender<SheduleChannelItem>> = Constructible::new();
        START.call_once(|| {
            thread::Builder::new()
                .name("spawn_after".into())
                .spawn(move || {
                    let (tx, rx) = channel::bounded(0);
                    SCHEDULE.pin(tx).expect("spawn_after] Can't pin the channel");
                    type Task = Pin<Box<dyn Future03<Output = ()> + Send + 'static>>;
                    let mut tasks: BTreeMap<Duration, Vec<Task>> = BTreeMap::new();
                    let mut ready = Vec::with_capacity(4);
                    loop {
                        let now = Duration::from_secs_f64(now_float());
                        let mut next_stop = Duration::from_secs_f64(0.1);
                        for (utc, _) in tasks.iter() {
                            if *utc <= now {
                                ready.push(*utc)
                            } else {
                                next_stop = *utc - now;
                                break;
                            }
                        }
                        for utc in ready.drain(..) {
                            let v = match tasks.remove(&utc) {
                                Some(v) => v,
                                None => continue,
                            };
                            //log! ("spawn_after] spawning " (v.len()) " tasks at " [utc]);
                            for f in v {
                                spawn(f)
                            }
                        }
                        let (utc, f) = match rx.recv_timeout(next_stop) {
                            Ok(t) => t,
                            Err(channel::RecvTimeoutError::Disconnected) => break,
                            Err(channel::RecvTimeoutError::Timeout) => continue,
                        };
                        tasks
                            .entry(Duration::from_secs_f64(utc))
                            .or_insert_with(Vec::new)
                            .push(f)
                    }
                })
                .expect("Can't spawn a spawn_after thread");
        });
        loop {
            match SCHEDULE.as_option() {
                None => {
                    thread::yield_now();
                    continue;
                },
                Some(tx) => {
                    tx.send((utc, Box::pin(future))).expect("Can't reach spawn_after");
                    break;
                },
            }
        }
    }

    /// A future that completes at a given time.  
    pub struct Timer {
        till_utc: f64,
    }

    impl Timer {
        pub fn till(till_utc: f64) -> Timer { Timer { till_utc } }
        pub fn sleep(seconds: f64) -> Timer {
            Timer {
                till_utc: now_float() + seconds,
            }
        }
        pub fn sleep_ms(ms: u32) -> Timer {
            let seconds = gstuff::duration_to_float(Duration::from_millis(ms as u64));
            Timer {
                till_utc: now_float() + seconds,
            }
        }
        pub fn till_utc(&self) -> f64 { self.till_utc }
    }

    impl Future03 for Timer {
        type Output = ();
        fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll03<Self::Output> {
            let delta = self.till_utc - now_float();
            if delta <= 0. {
                return Poll03::Ready(());
            }
            // NB: We should get a new `Waker` on every `poll` in case the future migrates between executors.
            // cf. https://rust-lang.github.io/async-book/02_execution/03_wakeups.html
            let waker = cx.waker().clone();
            spawn_after(self.till_utc, async move { waker.wake() });
            Poll03::Pending
        }
    }

    #[test]
    fn test_timer() {
        let started = now_float();
        let ti = Timer::sleep(0.2);
        let delta = now_float() - started;
        assert!(delta < 0.04, "{}", delta);
        super::block_on(ti);
        let delta = now_float() - started;
        println!("time delta is {}", delta);
        assert!(delta > 0.2);
        assert!(delta < 0.4)
    }
}

#[cfg(target_arch = "wasm32")] pub mod executor;

/// Returns a JSON error HyRes on a failure.
#[macro_export]
macro_rules! try_h {
    ($e: expr) => {
        match $e {
            Ok(ok) => ok,
            Err(err) => return $crate::rpc_err_response(500, &ERRL!("{}", err)),
        }
    };
}

/// Executes a GET request, returning the response status, headers and body.
pub async fn slurp_url(url: &str) -> SlurpRes {
    wio::slurp_req(try_s!(Request::builder().uri(url).body(Vec::new()))).await
}

#[test]
fn test_slurp_req() {
    let (status, headers, body) = block_on(slurp_url("https://httpbin.org/get")).unwrap();
    assert!(status.is_success(), "{:?} {:?} {:?}", status, headers, body);
}

/// Fetch URL by HTTPS and parse JSON response
pub async fn fetch_json<T>(url: &str) -> Result<T, String>
where
    T: serde::de::DeserializeOwned + Send + 'static,
{
    let result = try_s!(slurp_url(url).await);
    let result = try_s!(serde_json::from_slice(&result.2));
    Ok(result)
}

/// Send POST JSON HTTPS request and parse response
pub async fn post_json<T>(url: &str, json: String) -> Result<T, String>
where
    T: serde::de::DeserializeOwned + Send + 'static,
{
    let request = try_s!(Request::builder()
        .method("POST")
        .uri(url)
        .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
        .body(json.into()));

    let result = try_s!(wio::slurp_req(request).await);
    let result = try_s!(serde_json::from_slice(&result.2));
    Ok(result)
}

/// Wraps a JSON string into the `HyRes` RPC response future.
pub fn rpc_response<T>(status: u16, body: T) -> HyRes
where
    Vec<u8>: From<T>,
{
    let rf = match Response::builder()
        .status(status)
        .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
        .body(Vec::from(body))
    {
        Ok(r) => future::ok::<Response<Vec<u8>>, String>(r),
        Err(err) => {
            let err = ERRL!("{}", err);
            future::err::<Response<Vec<u8>>, String>(json!({ "error": err }).to_string())
        },
    };
    Box::new(rf)
}

#[derive(Serialize)]
struct ErrResponse {
    error: String,
}

/// Converts the given `err` message into the `{error: $err}` JSON string.
pub fn err_to_rpc_json_string(err: &str) -> String {
    let err = ErrResponse { error: err.to_owned() };
    json::to_string(&err).unwrap()
}

pub fn err_tp_rpc_json(error: String) -> Json { json::to_value(ErrResponse { error }).unwrap() }

/// Returns the `{error: $msg}` JSON response with the given HTTP `status`.
/// Also logs the error (if possible).
pub fn rpc_err_response(status: u16, msg: &str) -> HyRes {
    // TODO: Like in most other places, we should check for a thread-local access to the proper log here.
    // Might be a good idea to use emoji too, like "🤒" or "🤐" or "😕".
    // TODO: Consider turning this into a macros or merging with `try_h` in order to retain the `line!`.
    log! ({"RPC error response: {}", msg});

    rpc_response(status, err_to_rpc_json_string(msg))
}

/// A closure that would (re)start a `Future` to synchronize with an external resource in `RefreshedExternalResource`.
type ExternalResourceSync<R> =
    Box<dyn Fn() -> Box<dyn Future<Item = R, Error = String> + Send + 'static> + Send + 'static>;

/// Memory space accessible to the `Future` tail spawned by the `RefreshedExternalResource`.
struct RerShelf<R: Send + 'static> {
    /// The time when the `Future` generated by `sync` has filled this shell.
    time: f64,
    /// Results of the `sync`-generated `Future`.
    result: Result<R, String>,
}

/// Often we have an external resource that we need a fresh copy of.
/// (Or the other way around, when there is an external resource that we need to periodically update or synchronize with).
/// Particular property of such resources is that they might be unavailable,
/// might be slow due to resource overload or network congestion,
/// need to be resynchronized periodically
/// while being nice to the resource by maintaining rate limits.
///
/// Some of these resources are naturally singleton.
/// For exampe, we have only one "bittrex.com" and we need not multiple copies of its market data withing the process.
///
/// This helper here will organize the handling of such synchronization, periodically starting the synchronization `Future`,
/// restarting it on timeout, maintaining rate limits.
pub struct RefreshedExternalResource<R: Send + 'static> {
    sync: Mutex<ExternalResourceSync<R>>,
    /// Rate limit in the form of the desired number of seconds between the syncs.
    every_n_sec: f64,
    /// Start a new `Future` and drop the old one if it fails to finish after this number of seconds.
    timeout_sec: f64,
    /// The time (in f64 seconds) when we last (re)started the `sync`.
    /// We want `AtomicU64` but it isn't yet stable.
    last_start: AtomicUsize,
    shelf: Arc<Mutex<Option<RerShelf<R>>>>,
    /// The `Future`s interested in the next update.  
    /// When there is an updated the `Task::notify` gets invoked once and then the `Task` is removed from the `listeners` list.
    listeners: Arc<Mutex<Vec<Task>>>,
}
impl<R: Send + 'static> RefreshedExternalResource<R> {
    /// New instance of the external resource tracker.
    ///
    /// * `every_n_sec` - Desired number of seconds between the syncs.
    /// * `timeout_sec` - Start a new `sync` and drop the old `Future` if it fails to finish after this number of seconds.
    ///                   Automatically bumped to be at least `every_n_sec` large.
    /// * `sync` - Generates the `Future` that should synchronize with the external resource in background.
    ///            Note that we'll tail the `Future`, polling the tail from the shared asynchronous reactor;
    ///            *spawn* the `Future` onto a different reactor if the shared asynchronous reactor is not the best option.
    pub fn new(every_n_sec: f64, timeout_sec: f64, sync: ExternalResourceSync<R>) -> RefreshedExternalResource<R> {
        assert_eq!(size_of::<usize>(), 8);
        RefreshedExternalResource {
            sync: Mutex::new(sync),
            every_n_sec,
            timeout_sec: timeout_sec.max(every_n_sec),
            last_start: AtomicUsize::new(0f64.to_bits() as usize),
            shelf: Arc::new(Mutex::new(None)),
            listeners: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn add_listeners(&self, mut tasks: Vec<Task>) -> Result<(), String> {
        let mut listeners = try_s!(self.listeners.lock());
        listeners.append(&mut tasks);
        Ok(())
    }

    /// Performs the maintenance operations necessary to periodically refresh the resource.
    pub fn tick(&self) -> Result<(), String> {
        let now = now_float();
        let last_finish = match *try_s!(self.shelf.lock()) {
            Some(ref rer_shelf) => rer_shelf.time,
            None => 0.,
        };
        let last_start = f64::from_bits(self.last_start.load(Ordering::Relaxed) as u64);

        if now - last_start > self.timeout_sec || (last_finish > last_start && now - last_start > self.every_n_sec) {
            self.last_start.store(now.to_bits() as usize, Ordering::Relaxed);
            let sync = try_s!(self.sync.lock());
            let f = (*sync)();
            let shelf_tx = self.shelf.clone();
            let listeners = self.listeners.clone();
            let f = f.then(move |result| -> Result<(), ()> {
                let mut shelf = match shelf_tx.lock() {
                    Ok(l) => l,
                    Err(err) => {
                        log! ({"RefreshedExternalResource::tick] Can't lock the shelf: {}", err});
                        return Err(());
                    },
                };
                let shelf_time = match *shelf {
                    Some(ref r) => r.time,
                    None => 0.,
                };
                if now > shelf_time {
                    // This check prevents out-of-order shelf updates.
                    *shelf = Some(RerShelf {
                        time: now_float(),
                        result,
                    });
                    drop(shelf); // Don't hold the lock unnecessarily.
                    {
                        let mut listeners = match listeners.lock() {
                            Ok(l) => l,
                            Err(err) => {
                                log! ({"RefreshedExternalResource::tick] Can't lock the listeners: {}", err});
                                return Err(());
                            },
                        };
                        for task in listeners.drain(..) {
                            task.notify()
                        }
                    }
                }
                Ok(())
            });
            executor::spawn(f.compat().map(|_| ())); // Polls `f` in background.
        }

        Ok(())
    }

    /// The time, in seconds since UNIX epoch, when the refresh `Future` resolved.
    pub fn last_finish(&self) -> Result<f64, String> {
        Ok(match *try_s!(self.shelf.lock()) {
            Some(ref rer_shelf) => rer_shelf.time,
            None => 0.,
        })
    }

    pub fn with_result<V, F: FnMut(Option<&Result<R, String>>) -> Result<V, String>>(
        &self,
        mut cb: F,
    ) -> Result<V, String> {
        let shelf = try_s!(self.shelf.lock());
        match *shelf {
            Some(ref rer_shelf) => cb(Some(&rer_shelf.result)),
            None => cb(None),
        }
    }
}

#[derive(Clone, Debug)]
pub struct P2PMessage {
    pub from: SocketAddr,
    pub content: String,
}

impl P2PMessage {
    pub fn from_string_with_default_addr(content: String) -> P2PMessage {
        P2PMessage {
            from: SocketAddr::new([0; 4].into(), 0),
            content,
        }
    }

    pub fn from_serialize_with_default_addr<T: serde::Serialize>(msg: &T) -> P2PMessage {
        P2PMessage {
            from: SocketAddr::new([0; 4].into(), 0),
            content: serde_json::to_string(msg).unwrap(),
        }
    }
}

#[derive(Debug)]
pub struct QueuedCommand {
    pub response_sock: i32,
    pub stats_json_only: i32,
    pub queue_id: u32,
    pub msg: P2PMessage,
    // retstrp: *mut *mut c_char,
}

pub fn var(name: &str) -> Result<String, String> {
    /// Obtains the environment variable `name` from the host, copying it into `rbuf`.
    /// Returns the length of the value copied to `rbuf` or -1 if there was an error.
    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(raw_module = "../../../js/defined-in-js.js")]
    extern "C" {
        pub fn host_env(name: *const c_char, nameˡ: i32, rbuf: *mut c_char, rcap: i32) -> i32;
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        match std::env::var(name) {
            Ok(v) => Ok(v),
            Err(_err) => ERR!("No {}", name),
        }
    }

    #[cfg(target_arch = "wasm32")]
    {
        // Get the environment variable from the host.
        use std::str::from_utf8;

        let mut buf: [u8; 4096] = unsafe { zeroed() };
        let rc = host_env(
            name.as_ptr() as *const c_char,
            name.len() as i32,
            buf.as_mut_ptr() as *mut c_char,
            buf.len() as i32,
        );
        if rc <= 0 {
            return ERR!("No {}", name);
        }
        let s = try_s!(from_utf8(&buf[0..rc as usize]));
        Ok(String::from(s))
    }
}

/// TODO make it wasm32 only
/// #[cfg(not(target_arch = "wasm32"))]
pub fn block_on<F>(f: F) -> F::Output
where
    F: Future03,
{
    if var("TRACE_BLOCK_ON").map(|v| v == "true") == Ok(true) {
        let mut trace = String::with_capacity(4096);
        stack_trace(&mut stack_trace_frame, &mut |l| trace.push_str(l));
        log!("block_on at\n"(trace));
    }

    futures::executor::block_on(f)
}

use backtrace::SymbolName;

#[cfg(target_arch = "wasm32")]
pub fn now_ms() -> u64 { js_sys::Date::now() as u64 }

#[cfg(target_arch = "wasm32")]
pub fn now_float() -> f64 {
    use gstuff::duration_to_float;
    use std::time::Duration;
    duration_to_float(Duration::from_millis(now_ms()))
}

#[cfg(not(target_arch = "wasm32"))]
pub fn slurp(path: &dyn AsRef<Path>) -> Result<Vec<u8>, String> { Ok(gstuff::slurp(path)) }

#[cfg(not(target_arch = "wasm32"))]
pub fn safe_slurp(path: &dyn AsRef<Path>) -> Result<Vec<u8>, String> {
    let mut file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(ref err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return ERR!("Can't open {:?}: {}", path.as_ref(), err),
    };
    let mut buf = Vec::new();
    try_s!(file.read_to_end(&mut buf));
    Ok(buf)
}

#[cfg(target_arch = "wasm32")]
pub fn slurp(path: &dyn AsRef<Path>) -> Result<Vec<u8>, String> {
    use std::mem::MaybeUninit;

    #[wasm_bindgen(raw_module = "../../../js/defined-in-js.js")]
    extern "C" {
        pub fn host_slurp(path_p: *const c_char, path_l: i32, rbuf: *mut c_char, rcap: i32) -> i32;
    }

    let path = try_s!(path.as_ref().to_str().ok_or("slurp: path not unicode"));
    let mut rbuf: [u8; 262144] = unsafe { MaybeUninit::uninit().assume_init() };
    let rc = host_slurp(
        path.as_ptr() as *const c_char,
        path.len() as i32,
        rbuf.as_mut_ptr() as *mut c_char,
        rbuf.len() as i32,
    );
    if rc < 0 {
        return ERR!("!host_slurp: {}", rc);
    }
    Ok(Vec::from(&rbuf[..rc as usize]))
}

#[cfg(not(target_arch = "wasm32"))]
pub fn temp_dir() -> PathBuf { env::temp_dir() }

#[cfg(target_arch = "wasm32")]
pub fn temp_dir() -> PathBuf {
    #[wasm_bindgen(raw_module = "../../../js/defined-in-js.js")]
    extern "C" {
        pub fn temp_dir(rbuf: *mut c_char, rcap: i32) -> i32;
    }
    let mut buf: [u8; 4096] = unsafe { zeroed() };
    let rc = temp_dir(buf.as_mut_ptr() as *mut c_char, buf.len() as i32);
    if rc <= 0 {
        panic!("!temp_dir")
    }
    let path = std::str::from_utf8(&buf[0..rc as usize]).unwrap();
    Path::new(path).into()
}

#[cfg(not(target_arch = "wasm32"))]
pub fn remove_file(path: &dyn AsRef<Path>) -> Result<(), String> {
    try_s!(fs::remove_file(path));
    Ok(())
}

#[cfg(target_arch = "wasm32")]
pub fn remove_file(path: &dyn AsRef<Path>) -> Result<(), String> {
    #[wasm_bindgen(raw_module = "../../../js/defined-in-js.js")]
    extern "C" {
        pub fn host_rm(ptr: *const c_char, len: i32) -> i32;
    }

    let path = try_s!(path.as_ref().to_str().ok_or("Non-unicode path"));
    let rc = host_rm(path.as_ptr() as *const c_char, path.len() as i32);
    if rc != 0 {
        return ERR!("!host_rm: {}", rc);
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
pub fn write(path: &dyn AsRef<Path>, contents: &dyn AsRef<[u8]>) -> Result<(), String> {
    try_s!(fs::write(path, contents));
    Ok(())
}

#[cfg(target_arch = "wasm32")]
pub fn write(path: &dyn AsRef<Path>, contents: &dyn AsRef<[u8]>) -> Result<(), String> {
    #[wasm_bindgen(raw_module = "../../../js/defined-in-js.js")]
    extern "C" {
        pub fn host_write(path_p: *const c_char, path_l: i32, ptr: *const c_char, len: i32) -> i32;
    }

    let path = try_s!(path.as_ref().to_str().ok_or("Non-unicode path"));
    let content = contents.as_ref();
    let rc = host_write(
        path.as_ptr() as *const c_char,
        path.len() as i32,
        content.as_ptr() as *const c_char,
        content.len() as i32,
    );
    if rc != 0 {
        return ERR!("!host_write: {}", rc);
    }
    Ok(())
}

/// Read a folder and return a list of files with their last-modified ms timestamps.
#[cfg(not(target_arch = "wasm32"))]
pub fn read_dir(dir: &dyn AsRef<Path>) -> Result<Vec<(u64, PathBuf)>, String> {
    use std::time::UNIX_EPOCH;

    let entries = try_s!(dir.as_ref().read_dir())
        .filter_map(|dir_entry| {
            let entry = match dir_entry {
                Ok(ent) => ent,
                Err(e) => {
                    log!("Error " (e) " reading from dir " (dir.as_ref().display()));
                    return None;
                },
            };

            let metadata = match entry.metadata() {
                Ok(m) => m,
                Err(e) => {
                    log!("Error " (e) " getting file " (entry.path().display()) " meta");
                    return None;
                },
            };

            let m_time = match metadata.modified() {
                Ok(time) => time,
                Err(e) => {
                    log!("Error " (e) " getting file " (entry.path().display()) " m_time");
                    return None;
                },
            };

            let lm = m_time.duration_since(UNIX_EPOCH).expect("!duration_since").as_millis();
            assert!(lm < u64::max_value() as u128);
            let lm = lm as u64;

            let path = entry.path();
            if path.extension() == Some(OsStr::new("json")) {
                Some((lm, path))
            } else {
                None
            }
        })
        .collect();

    Ok(entries)
}

#[cfg(target_arch = "wasm32")]
pub fn read_dir(dir: &dyn AsRef<Path>) -> Result<Vec<(u64, PathBuf)>, String> {
    use std::mem::MaybeUninit;

    #[wasm_bindgen(raw_module = "../../../js/defined-in-js.js")]
    extern "C" {
        pub fn host_read_dir(path_p: *const c_char, path_l: i32, rbuf: *mut c_char, rcap: i32) -> i32;
    }

    let path = try_s!(dir.as_ref().to_str().ok_or("read_dir: dir path not unicode"));
    let mut rbuf: [u8; 262144] = unsafe { MaybeUninit::uninit().assume_init() };
    let rc = host_read_dir(
        path.as_ptr() as *const c_char,
        path.len() as i32,
        rbuf.as_mut_ptr() as *mut c_char,
        rbuf.len() as i32,
    );
    if rc <= 0 {
        return ERR!("!host_read_dir: {}", rc);
    }
    let jens: Vec<(u64, String)> = try_s!(json::from_slice(&rbuf[..rc as usize]));

    let mut entries: Vec<(u64, PathBuf)> = Vec::with_capacity(jens.len());
    for (lm, name) in jens {
        let path = dir.as_ref().join(name);
        entries.push((lm, path))
    }

    Ok(entries)
}

/// If the `MM_LOG` variable is present then tries to open that file.  
/// Prints a warning to `stdout` if there's a problem opening the file.  
/// Returns `None` if `MM_LOG` variable is not present or if the specified path can't be opened.
#[cfg(not(target_arch = "wasm32"))]
fn open_log_file() -> Option<fs::File> {
    let mm_log = match var("MM_LOG") {
        Ok(v) => v,
        Err(_) => return None,
    };

    // For security reasons we want the log path to always end with ".log".
    if !mm_log.ends_with(".log") {
        println!("open_log_file] MM_LOG doesn't end with '.log'");
        return None;
    }

    match fs::OpenOptions::new().append(true).create(true).open(&mm_log) {
        Ok(f) => Some(f),
        Err(err) => {
            println!("open_log_file] Can't open {}: {}", mm_log, err);
            None
        },
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub fn writeln(line: &str) {
    use std::panic::catch_unwind;

    lazy_static! {
        static ref LOG_FILE: Mutex<Option<fs::File>> = Mutex::new(open_log_file());
    }

    // `catch_unwind` protects the tests from error
    //
    //     thread 'CORE' panicked at 'cannot access stdout during shutdown'
    //
    // (which might be related to https://github.com/rust-lang/rust/issues/29488).
    let _ = catch_unwind(|| {
        if let Ok(mut log_file) = LOG_FILE.lock() {
            if let Some(ref mut log_file) = *log_file {
                let _ = witeln!(log_file, (line));
                return;
            }
        }
        println!("{}", line);
    });
}

#[cfg(target_arch = "wasm32")]
static mut PROCESS_LOG_TAIL: [u8; 0x10000] = [0; 0x10000];

#[cfg(target_arch = "wasm32")]
static TAIL_CUR: Atomic<usize> = Atomic::new(0);

/// Keep a tail of the log in RAM for the integration tests.
#[cfg(target_arch = "wasm32")]
pub fn append_log_tail(line: &str) {
    unsafe {
        if line.len() < PROCESS_LOG_TAIL.len() {
            let posⁱ = TAIL_CUR.load(Ordering::Relaxed);
            let posⱼ = posⁱ + line.len();
            let (posˢ, posⱼ) = if posⱼ > PROCESS_LOG_TAIL.len() {
                (0, line.len())
            } else {
                (posⁱ, posⱼ)
            };
            if TAIL_CUR
                .compare_exchange(posⁱ, posⱼ, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                for (cur, ix) in (posˢ..posⱼ).zip(0..line.len()) {
                    PROCESS_LOG_TAIL[cur] = line.as_bytes()[ix]
                }
            }
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub fn writeln(line: &str) {
    use web_sys::console;
    console::log_1(&line.into());
    append_log_tail(line);
}

pub fn small_rng() -> SmallRng { SmallRng::seed_from_u64(now_ms()) }

/// Ask the WASM host to send HTTP request to the native helpers.
/// Returns request ID used to wait for the reply.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(raw_module = "../../../js/defined-in-js.js")]
extern "C" {
    fn http_helper_if(helper: *const u8, helper_len: i32, payload: *const u8, payload_len: i32, timeout_ms: i32)
        -> i32;
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(raw_module = "../../../js/defined-in-js.js")]
extern "C" {
    /// Check with the WASM host to see if the given HTTP request is ready.
    ///
    /// Returns the amount of bytes copied to rbuf,  
    /// or `-1` if the request is not yet finished,  
    /// or `0 - amount of bytes` in case the intended size was larger than the `rcap`.
    ///
    /// The bytes copied to rbuf are in the bencode format,
    /// `{status: $number, ct: $bytes, cs: $bytes, body: $bytes}`
    /// (the `HelperResponse`).
    ///
    /// * `helper_request_id` - Request ID previously returned by `http_helper_if`.
    /// * `rbuf` - The buffer to copy the response payload into if the request is finished.
    /// * `rcap` - The size of the `rbuf` buffer.
    pub fn http_helper_check(helper_request_id: i32, rbuf: *mut u8, rcap: i32) -> i32;
}

lazy_static! {
    /// Maps helper request ID to the corresponding Waker,
    /// allowing WASM host to wake the `HelperReply`.
    static ref HELPER_REQUESTS: Mutex<HashMap<i32, Waker>> = Mutex::new (HashMap::new());
}

/// WASM host invokes this method to signal the readiness of the HTTP request.
#[no_mangle]
#[cfg(target_arch = "wasm32")]
pub extern "C" fn http_ready(helper_request_id: i32) {
    let mut helper_requests = HELPER_REQUESTS.lock().unwrap();
    if let Some(waker) = helper_requests.remove(&helper_request_id) {
        waker.wake()
    }
}

#[derive(Deserialize, Debug)]
pub struct HelperResponse {
    pub status: u32,
    #[serde(rename = "ct")]
    pub content_type: Option<ByteBuf>,
    #[serde(rename = "cs")]
    pub checksum: Option<ByteBuf>,
    pub body: ByteBuf,
}
/// Mostly used to log the errors coming from the other side.
impl fmt::Display for HelperResponse {
    fn fmt(&self, ft: &mut fmt::Formatter) -> fmt::Result {
        wite! (ft, (self.status) ", " (binprint (&self.body, b'.')))
    }
}

#[cfg(target_arch = "wasm32")]
pub async fn helperᶜ(helper: &'static str, args: Vec<u8>) -> Result<Vec<u8>, String> {
    use serde_bencode::de::from_bytes as bdecode;

    let helper_request_id = http_helper_if(
        helper.as_ptr(),
        helper.len() as i32,
        args.as_ptr(),
        args.len() as i32,
        9999,
    );

    struct HelperReply {
        helper_request_id: i32,
    }
    impl std::future::Future for HelperReply {
        type Output = Result<Vec<u8>, String>;
        fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll03<Self::Output> {
            let mut buf: [u8; 65535] = unsafe { std::mem::MaybeUninit::uninit().assume_init() };
            let rlen = http_helper_check(self.helper_request_id, buf.as_mut_ptr(), buf.len() as i32);
            if rlen < -1 {
                // Response is larger than capacity.
                return Poll03::Ready(ERR!("Helper result is too large ({})", rlen));
            }
            if rlen >= 0 {
                return Poll03::Ready(Ok(Vec::from(&buf[0..rlen as usize])));
            }

            // NB: Need a fresh waker each time `Pending` is returned, to support switching tasks.
            // cf. https://rust-lang.github.io/async-book/02_execution/03_wakeups.html
            let waker = cx.waker().clone();
            HELPER_REQUESTS.lock().unwrap().insert(self.helper_request_id, waker);

            Poll03::Pending
        }
    }
    impl Drop for HelperReply {
        fn drop(&mut self) { HELPER_REQUESTS.lock().unwrap().remove(&self.helper_request_id); }
    }
    let rv: Vec<u8> = try_s!(HelperReply { helper_request_id }.await);
    let rv: HelperResponse = try_s!(bdecode(&rv));
    if rv.status != 200 {
        return ERR!("!{}: {}", helper, rv);
    }
    // TODO: Check `rv.checksum` if present.
    Ok(rv.body.into_vec())
}

#[derive(Serialize, Deserialize)]
pub struct BroadcastP2pMessageArgs {
    pub ctx: u32,
    pub msg: String,
}

#[derive(Debug, Clone)]
/// Ordered from low to height inclusive range.
pub struct OrdRange<T>(RangeInclusive<T>);

impl<T> Deref for OrdRange<T> {
    type Target = RangeInclusive<T>;

    fn deref(&self) -> &Self::Target { &self.0 }
}

impl<T: PartialOrd> OrdRange<T> {
    /// Construct the OrderedRange from the start-end pair.
    pub fn new(start: T, end: T) -> Result<Self, String> {
        if start > end {
            return Err("".into());
        }

        Ok(Self(start..=end))
    }
}

impl<T: Copy> OrdRange<T> {
    /// Flatten a start-end pair into the vector.
    pub fn flatten(&self) -> Vec<T> { vec![*self.start(), *self.end()] }
}

/// Invokes callback `cb_id` in the WASM host, passing a `(ptr,len)` string to it.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(raw_module = "../../../js/defined-in-js.js")]
extern "C" {
    pub fn call_back(cb_id: i32, ptr: *const c_char, len: i32);
}
pub mod for_tests;

fn without_trailing_zeroes(decimal: &str, dot: usize) -> &str {
    let mut pos = decimal.len() - 1;
    loop {
        let ch = decimal.as_bytes()[pos];
        if ch != b'0' {
            break &decimal[0..=pos];
        }
        if pos == dot {
            break &decimal[0..pos];
        }
        pos -= 1
    }
}

/// Round half away from zero (aka commercial rounding).
pub fn round_to(bd: &BigDecimal, places: u8) -> String {
    // Normally we'd do
    //
    //     let divisor = pow (10, places)
    //     round (self * divisor) / divisor
    //
    // But we don't have a `round` function in `BigDecimal` at present, so we're on our own.

    let bds = format!("{}", bd);
    let bda = bds.as_bytes();
    let dot = bda.iter().position(|&ch| ch == b'.');
    let dot = match dot {
        Some(dot) => dot,
        None => return bds,
    };

    if bda.len() - dot <= places as usize {
        return String::from(without_trailing_zeroes(&bds, dot));
    }

    let mut pos = bda.len() - 1;
    let mut ch = bda[pos];
    let mut prev_digit = 0;
    loop {
        let digit = ch - b'0';
        let rounded = if prev_digit > 5 { digit + 1 } else { digit };
        //println! ("{} at {}: prev_digit {}, digit {}, rounded {}", bds, pos, prev_digit, digit, rounded);

        if pos < dot {
            //println! ("{}, pos < dot, stopping at pos {}", bds, pos);
            let mut integer: i64 = (&bds[0..=pos]).parse().unwrap();
            if prev_digit > 5 {
                if bda[0] == b'-' {
                    integer = integer.checked_sub(1).unwrap()
                } else {
                    integer = integer.checked_add(1).unwrap()
                }
            }
            return format!("{}", integer);
        }

        if pos == dot + places as usize && rounded < 10 {
            //println! ("{}, stopping at pos {}", bds, pos);
            break format!("{}{}", &bds[0..pos], rounded);
        }

        pos -= 1;
        if pos == dot {
            pos -= 1
        } // Skip over the dot.
        ch = bda[pos];
        prev_digit = rounded
    }
}

#[test]
fn test_round_to() {
    assert_eq!(round_to(&BigDecimal::from(0.999), 2), "1");
    assert_eq!(round_to(&BigDecimal::from(-0.999), 2), "-1");

    assert_eq!(round_to(&BigDecimal::from(10.999), 2), "11");
    assert_eq!(round_to(&BigDecimal::from(-10.999), 2), "-11");

    assert_eq!(round_to(&BigDecimal::from(99.9), 1), "99.9");
    assert_eq!(round_to(&BigDecimal::from(-99.9), 1), "-99.9");

    assert_eq!(round_to(&BigDecimal::from(99.9), 0), "100");
    assert_eq!(round_to(&BigDecimal::from(-99.9), 0), "-100");

    let ouch = BigDecimal::from(1) / BigDecimal::from(7);
    assert_eq!(round_to(&ouch, 3), "0.143");

    let ouch = BigDecimal::from(1) / BigDecimal::from(3);
    assert_eq!(round_to(&ouch, 0), "0");
    assert_eq!(round_to(&ouch, 1), "0.3");
    assert_eq!(round_to(&ouch, 2), "0.33");
    assert_eq!(round_to(&ouch, 9), "0.333333333");

    assert_eq!(round_to(&BigDecimal::from(0.123), 99), "0.123");
    assert_eq!(round_to(&BigDecimal::from(-0.123), 99), "-0.123");

    assert_eq!(round_to(&BigDecimal::from(0), 99), "0");
    assert_eq!(round_to(&BigDecimal::from(-0), 99), "0");

    assert_eq!(round_to(&BigDecimal::from(0.123), 0), "0");
    assert_eq!(round_to(&BigDecimal::from(-0.123), 0), "0");

    assert_eq!(round_to(&BigDecimal::from(0), 0), "0");
    assert_eq!(round_to(&BigDecimal::from(-0), 0), "0");
}

#[cfg(not(target_arch = "wasm32"))]
pub fn new_uuid() -> Uuid { Uuid::new_v4() }

#[cfg(target_arch = "wasm32")]
pub fn new_uuid() -> Uuid {
    use rand::RngCore;
    use uuid::{Builder, Variant, Version};

    let mut rng = small_rng();
    let mut bytes = [0; 16];

    rng.fill_bytes(&mut bytes);

    Builder::from_bytes(bytes)
        .set_variant(Variant::RFC4122)
        .set_version(Version::Random)
        .build()
}

/// Get only the first line of the error.
/// Generally, the `JsValue` error contains the stack trace of an error.
/// This function cuts off the stack trace.
#[cfg(target_arch = "wasm32")]
pub fn stringify_js_error(error: &JsValue) -> String {
    format!("{:?}", error)
        .lines()
        .next()
        .map(|e| e.to_owned())
        .unwrap_or_default()
}

/// The function helper for the `WasmUnwrapExt`, `WasmUnwrapErrExt` traits.
#[cfg(target_arch = "wasm32")]
#[track_caller]
fn caller_file_line() -> (&'static str, u32) {
    let location = std::panic::Location::caller();
    let file = gstuff::filename(location.file());
    let line = location.line();
    (file, line)
}

#[cfg(target_arch = "wasm32")]
pub trait WasmUnwrapExt<T> {
    fn unwrap_w(self) -> T;
    fn expect_w(self, description: &str) -> T;
}

#[cfg(target_arch = "wasm32")]
pub trait WasmUnwrapErrExt<E> {
    fn unwrap_err_w(self) -> E;
    fn expect_err_w(self, description: &str) -> E;
}

#[cfg(target_arch = "wasm32")]
impl<T, E: fmt::Debug> WasmUnwrapExt<T> for Result<T, E> {
    #[track_caller]
    fn unwrap_w(self) -> T {
        match self {
            Ok(t) => t,
            Err(e) => {
                let (file, line) = caller_file_line();
                let error = format!(
                    "{}:{}] 'Result::unwrap_w' called on an 'Err' value: {:?}",
                    file, line, e
                );
                wasm_bindgen::throw_str(&error)
            },
        }
    }

    #[track_caller]
    fn expect_w(self, description: &str) -> T {
        match self {
            Ok(t) => t,
            Err(e) => {
                let (file, line) = caller_file_line();
                let error = format!("{}:{}] {}: {:?}", file, line, description, e);
                wasm_bindgen::throw_str(&error)
            },
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl<T> WasmUnwrapExt<T> for Option<T> {
    #[track_caller]
    fn unwrap_w(self) -> T {
        match self {
            Some(t) => t,
            None => {
                let (file, line) = caller_file_line();
                let error = format!("{}:{}] 'Option::unwrap_w' called on a 'None' value", file, line);
                wasm_bindgen::throw_str(&error)
            },
        }
    }

    #[track_caller]
    fn expect_w(self, description: &str) -> T {
        match self {
            Some(t) => t,
            None => {
                let (file, line) = caller_file_line();
                let error = format!("{}:{}] {}", file, line, description);
                wasm_bindgen::throw_str(&error)
            },
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl<T: fmt::Debug, E> WasmUnwrapErrExt<E> for Result<T, E> {
    #[track_caller]
    fn unwrap_err_w(self) -> E {
        match self {
            Ok(t) => {
                let (file, line) = caller_file_line();
                let error = format!(
                    "{}:{}] 'Result::unwrap_err_w' called on an 'Ok' value: {:?}",
                    file, line, t
                );
                wasm_bindgen::throw_str(&error)
            },
            Err(e) => e,
        }
    }

    #[track_caller]
    fn expect_err_w(self, description: &str) -> E {
        match self {
            Ok(t) => {
                let (file, line) = caller_file_line();
                let error = format!("{}:{}] {}: {:?}", file, line, description, t);
                wasm_bindgen::throw_str(&error)
            },
            Err(e) => e,
        }
    }
}

#[cfg(target_arch = "wasm32")]
#[track_caller]
pub fn panic_w(description: &str) {
    let (file, line) = caller_file_line();
    let error = format!("{}:{}] 'panic_w' called: {:?}", file, line, description);
    wasm_bindgen::throw_str(&error)
}

pub fn first_char_to_upper(input: &str) -> String {
    let mut v: Vec<char> = input.chars().collect();
    if let Some(c) = v.first_mut() {
        c.make_ascii_uppercase()
    }
    v.into_iter().collect()
}

#[test]
fn test_first_char_to_upper() {
    assert_eq!("", first_char_to_upper(""));
    assert_eq!("K", first_char_to_upper("k"));
    assert_eq!("Komodo", first_char_to_upper("komodo"));
    assert_eq!(".komodo", first_char_to_upper(".komodo"));
}

pub fn json_dir_entries(path: &dyn AsRef<Path>) -> Result<Vec<DirEntry>, String> {
    Ok(try_s!(path.as_ref().read_dir())
        .filter_map(|dir_entry| {
            let entry = match dir_entry {
                Ok(ent) => ent,
                Err(e) => {
                    log!("Error " (e) " reading from dir " (path.as_ref().display()));
                    return None;
                },
            };

            if entry.path().extension() == Some(OsStr::new("json")) {
                Some(entry)
            } else {
                None
            }
        })
        .collect())
}

/// Calculates the median of the set represented as slice
pub fn median<T: Add<Output = T> + Div<Output = T> + Copy + From<u8> + Ord>(input: &mut [T]) -> Option<T> {
    // median is undefined on empty sets
    if input.is_empty() {
        return None;
    }
    input.sort();
    let median_index = input.len() / 2;
    if input.len() % 2 == 0 {
        Some((input[median_index - 1] + input[median_index]) / T::from(2u8))
    } else {
        Some(input[median_index])
    }
}

#[test]
fn test_median() {
    let mut input = [3, 2, 1];
    let expected = Some(2u32);
    let actual = median(&mut input);
    assert_eq!(expected, actual);

    let mut input = [3, 1];
    let expected = Some(2u32);
    let actual = median(&mut input);
    assert_eq!(expected, actual);

    let mut input = [1, 3, 2, 8, 10];
    let expected = Some(3u32);
    let actual = median(&mut input);
    assert_eq!(expected, actual);
}

pub fn calc_total_pages(entries_len: usize, limit: usize) -> usize {
    if limit == 0 {
        return 0;
    }
    let pages_num = entries_len / limit;
    if entries_len % limit == 0 {
        pages_num
    } else {
        pages_num + 1
    }
}

#[test]
fn test_calc_total_pages() {
    assert_eq!(0, calc_total_pages(0, 0));
    assert_eq!(0, calc_total_pages(0, 1));
    assert_eq!(0, calc_total_pages(0, 100));
    assert_eq!(1, calc_total_pages(1, 1));
    assert_eq!(2, calc_total_pages(16, 8));
    assert_eq!(2, calc_total_pages(15, 8));
}
