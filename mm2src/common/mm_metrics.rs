use crate::executor::{spawn, Timer};
use crate::log::{LogState, LogArc, LogWeak, Tag};
use gstuff::Constructible;
use hdrhistogram::Histogram;
use metrics_core::{Builder, Drain, Key, Label, Observe, Observer, ScopedString};
use metrics_runtime::{observers::JsonBuilder, Receiver};
pub use metrics_runtime::Sink;
use metrics_util::{parse_quantiles, Quantile};
use serde_json::{self as json, Value as Json};
use std::collections::HashMap;
use std::fmt::Write as WriteFmt;
use std::ops::Deref;
use std::slice::Iter;
use std::sync::{Arc, Weak};

/// Increment counter if an MmArc is not dropped yet and metrics system is initialized already.
#[macro_export]
macro_rules! mm_counter {
    ($metrics_weak:expr, $name:expr, $value:expr) => {{
        if let Some(mut sink) = $crate::mm_metrics::try_sink_from_metrics(&$metrics_weak) {
            sink.increment_counter($name, $value);
        }
    }};

    ($metrics_weak:expr, $name:expr, $value:expr, $($labels:tt)*) => {{
        use metrics::labels;
        if let Some(mut sink) = $crate::mm_metrics::try_sink_from_metrics(&$metrics_weak) {
            let labels = labels!( $($labels)* );
            sink.increment_counter_with_labels($name, $value, labels);
        }
    }};
}

/// Update gauge if an MmArc is not dropped yet and metrics system is initialized already.
#[macro_export]
macro_rules! mm_gauge {
    ($metrics_weak:expr, $name:expr, $value:expr) => {{
        if let Some(mut sink) = $crate::mm_metrics::try_sink_from_metrics(&$metrics_weak) {
            sink.update_gauge($name, $value);
        }
    }};

    ($metrics_weak:expr, $name:expr, $value:expr, $($labels:tt)*) => {{
        use metrics::labels;
        if let Some(mut sink) = $crate::mm_metrics::try_sink_from_metrics(&$metrics_weak) {
            let labels = labels!( $($labels)* );
            sink.update_gauge_with_labels($name, $value, labels);
        }
    }};
}

/// Pass new timing value if an MmArc is not dropped yet and metrics system is initialized already.
#[macro_export]
macro_rules! mm_timing {
    ($metrics_weak:expr, $name:expr, $start:expr, $end:expr) => {{
        if let Some(mut sink) = $crate::mm_metrics::try_sink_from_metrics(&$metrics_weak) {
            sink.record_timing($name, $start, $end);
        }
    }};

    ($metrics_weak:expr, $name:expr, $start:expr, $end:expr, $($labels:tt)*) => {{
        use metrics::labels;
        if let Some(mut sink) = $crate::mm_metrics::try_sink_from_metrics(&$metrics_weak) {
            let labels = labels!( $($labels)* );
            sink.record_timing_with_labels($name, $start, $end, labels);
        }
    }};
}

pub fn try_sink_from_metrics(weak: &MetricsWeak) -> Option<Sink> {
    let metrics = MetricsArc::from_weak(&weak)?;
    metrics.sink().ok()
}

/// Default quantiles are "min" and "max"
const QUANTILES: &[f64] = &[0.0, 1.0];

#[derive(Default)]
pub struct Metrics {
    /// `Receiver` receives and collect all the metrics sent through the `sink`.
    /// The `receiver` can be initialized only once time.
    receiver: Constructible<Receiver>,
}

impl Metrics {
    /// If the instance was not initialized yet, create the `receiver` else return an error.
    pub fn init(&self) -> Result<(), String> {
        if self.receiver.is_some() {
            return ERR!("metrics system is initialized already");
        }

        let receiver = try_s!(Receiver::builder().build());
        let _ = try_s!(self.receiver.pin(receiver));

        Ok(())
    }

    /// Create new Metrics instance and spawn the metrics recording into the log, else return an error.
    pub fn init_with_dashboard(&self, log_state: LogWeak, record_interval: f64) -> Result<(), String> {
        self.init()?;

        let controller = self.receiver.as_option().unwrap().controller();

        let observer = TagObserver::new(QUANTILES);
        let exporter = TagExporter { log_state, controller, observer };

        spawn(exporter.run(record_interval));

        Ok(())
    }

    /// Handle for sending metric samples.
    pub fn sink(&self) -> Result<Sink, String> {
        Ok(try_s!(self.try_receiver()).sink())
    }

    /// Collect the metrics as Json.
    pub fn collect_json(&self) -> Result<Json, String> {
        let receiver = try_s!(self.try_receiver());
        let controller = receiver.controller();

        // pretty_json is false by default
        let builder = JsonBuilder::new().set_quantiles(QUANTILES);
        let mut observer = builder.build();

        controller.observe(&mut observer);

        let string = observer.drain();

        Ok(try_s!(json::from_str(&string)))
    }

    fn try_receiver(&self) -> Result<&Receiver, String> {
        self.receiver.ok_or("metrics system is not initialized yet".into())
    }
}

#[derive(Clone)]
pub struct MetricsArc(pub Arc<Metrics>);

impl Deref for MetricsArc {
    type Target = Metrics;
    fn deref(&self) -> &Metrics {
        &*self.0
    }
}

impl MetricsArc {
    /// Create new `Metrics` instance
    pub fn new() -> MetricsArc {
        MetricsArc(Arc::new(Default::default()))
    }

    /// Try to obtain the `Metrics` from the weak pointer.
    pub fn from_weak(weak: &MetricsWeak) -> Option<MetricsArc> {
        weak.0.upgrade().map(|arc| MetricsArc(arc))
    }

    /// Create a weak pointer from `MetricsWeak`.
    pub fn weak(&self) -> MetricsWeak {
        MetricsWeak(Arc::downgrade(&self.0))
    }
}

pub struct MetricsWeak(pub Weak<Metrics>);

impl MetricsWeak {
    /// Create a default MmWeak without allocating any memory.
    pub fn new() -> MetricsWeak {
        MetricsWeak(Default::default())
    }

    pub fn dropped(&self) -> bool {
        self.0.strong_count() == 0
    }
}

type MetricName = ScopedString;

type MetricLabels = Vec<Label>;

type MetricNameValueMap = HashMap<MetricName, Integer>;

#[derive(Clone)]
enum Integer {
    Signed(i64),
    Unsigned(u64),
}

impl ToString for Integer {
    fn to_string(&self) -> String {
        match self {
            Integer::Signed(x) => format!("{}", x),
            Integer::Unsigned(x) => format!("{}", x),
        }
    }
}

struct PreparedMetric {
    tags: Vec<Tag>,
    message: String,
}

/// Observes metrics and histograms in Tag format.
struct TagObserver {
    /// Supported quantiles like Min, 0.5, 0.8, Max
    quantiles: Vec<Quantile>,
    /// Metric:Value pair matching an unique set of labels.
    metrics: HashMap<MetricLabels, MetricNameValueMap>,
    /// Histograms present set of time measurements and analysis over the measurements
    histograms: HashMap<Key, Histogram<u64>>,
}

impl TagObserver {
    fn new(quantiles: &[f64]) -> Self {
        TagObserver {
            quantiles: parse_quantiles(quantiles),
            metrics: Default::default(),
            histograms: Default::default(),
        }
    }

    fn prepare_metrics(&self) -> Vec<PreparedMetric> {
        self.metrics.iter()
            .map(|(labels, name_value_map)| {
                let mut tags = labels_to_tags(labels.iter());
                tags.extend(name_value_map_to_tags(name_value_map));
                let message = String::default();

                PreparedMetric { tags, message }
            })
            .collect()
    }

    fn prepare_histograms(&self) -> Vec<PreparedMetric> {
        self.histograms.iter()
            .map(|(key, hist)| {
                let mut tags = labels_to_tags(key.labels());
                tags.push(Tag { key: key.name().to_string(), val: None });
                let message = hist_to_message(hist, &self.quantiles);

                PreparedMetric { tags, message }
            })
            .collect()
    }

    fn insert_metric(&mut self, key: Key, value: Integer) {
        let (name, labels) = key.into_parts();
        self.metrics.entry(labels)
            .and_modify(|name_value_map| {
                name_value_map.insert(name.clone(), value.clone());
            })
            .or_insert({
                let mut name_value_map = HashMap::new();
                name_value_map.insert(name, value);
                name_value_map
            });
    }

    /// Clear metrics or histograms if it's necessary
    /// after an exporter has turned the observer's metrics and histograms.
    fn on_turned(&mut self) {
        // clear histograms because they can be duplicated
        self.histograms.clear();
        // don't clear metrics because the keys don't changes often
    }
}

impl Observer for TagObserver {
    fn observe_counter(&mut self, key: Key, value: u64) {
        self.insert_metric(key, Integer::Unsigned(value))
    }

    fn observe_gauge(&mut self, key: Key, value: i64) {
        self.insert_metric(key, Integer::Signed(value))
    }

    fn observe_histogram(&mut self, key: Key, values: &[u64]) {
        let entry = self.histograms
            .entry(key)
            .or_insert({
                // Use default significant figures value.
                // For more info on `sigfig` see the Historgam::new_with_bounds().
                let sigfig = 3;
                match Histogram::new(sigfig) {
                    Ok(x) => x,
                    Err(err) => {
                        ERRL!("failed to create histogram: {}", err);
                        // do nothing on error
                        return;
                    }
                }
            });

        for value in values {
            if let Err(err) = entry.record(*value) {
                ERRL!("failed to observe histogram value: {}", err);
            }
        }
    }
}

/// Exports metrics by converting them to a Tag format and log them using log::Status.
struct TagExporter<C>
{
    /// Using a weak reference by default in order to avoid circular references and leaks.
    log_state: LogWeak,
    /// Handle for acquiring metric snapshots.
    controller: C,
    /// Handle for converting snapshots into log.
    observer: TagObserver,
}

impl<C> TagExporter<C>
    where
        C: Observe {
    /// Run endless async loop
    async fn run(mut self, interval: f64) {
        loop {
            Timer::sleep(interval).await;
            self.turn();
        }
    }

    /// Observe metrics and histograms and record it into the log in Tag format
    fn turn(&mut self) {
        let log_state = match LogArc::from_weak(&self.log_state) {
            Some(x) => x,
            // MmCtx is dropped already
            _ => return
        };

        log!(">>>>>>>>>> DEX metrics <<<<<<<<<");

        // Observe means fill the observer's metrics and histograms with actual values
        self.controller.observe(&mut self.observer);

        for PreparedMetric { tags, message } in self.observer.prepare_metrics() {
            log_state.log_deref_tags("", tags, &message);
        }

        for PreparedMetric { tags, message } in self.observer.prepare_histograms() {
            log_state.log_deref_tags("", tags, &message);
        }

        self.observer.on_turned();
    }
}

fn labels_to_tags(labels: Iter<Label>) -> Vec<Tag> {
    labels
        .map(|label| Tag {
            key: label.key().to_string(),
            val: Some(label.value().to_string()),
        })
        .collect()
}

fn name_value_map_to_tags(name_value_map: &MetricNameValueMap) -> Vec<Tag> {
    name_value_map.iter()
        .map(|(key, value)| {
            Tag { key: key.to_string(), val: Some(value.to_string()) }
        })
        .collect()
}

fn hist_to_message(
    hist: &Histogram<u64>,
    quantiles: &[Quantile],
) -> String {
    let mut message = String::with_capacity(256);
    let fmt_quantiles = quantiles
        .iter()
        .map(|quantile| {
            let key = quantile.label().to_string();
            let val = hist.value_at_quantile(quantile.value());
            format!("{}={}", key, val)
        });

    match wite!(message,
                "count=" (hist.len())
                if quantiles.is_empty() { "" } else { " " }
                for q in fmt_quantiles { (q) } separated {' '}
    ) {
        Ok(_) => message,
        Err(err) => {
            log!("Error " (err) " on format hist to message");
            String::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{block_on, log::LogArc};
    use metrics_runtime::Delta;
    use super::*;

    #[test]
    fn test_initialization() {
        let log_state = LogArc::new(LogState::in_memory());
        let metrics = MetricsArc::new();

        // metrics system is not initialized yet
        assert!(metrics.sink().is_err());

        unwrap!(metrics.init());
        assert!(metrics.init().is_err());
        assert!(metrics.init_with_dashboard(log_state.weak(), 1.).is_err());

        let _ = unwrap!(metrics.sink());
    }

    #[test]
    #[ignore]
    fn test_dashboard() {
        let log_state = LogArc::new(LogState::in_memory());
        let metrics_shared = MetricsArc::new();
        let metrics = metrics_shared.weak();

        metrics_shared.init_with_dashboard(log_state.weak(), 5.).unwrap();
        let sink = metrics_shared.sink().unwrap();

        let start = sink.now();

        mm_counter!(metrics, "rpc.traffic.tx", 62, "coin" => "BTC");
        mm_counter!(metrics, "rpc.traffic.rx", 105, "coin"=> "BTC");

        mm_counter!(metrics, "rpc.traffic.tx", 54, "coin" => "KMD");
        mm_counter!(metrics, "rpc.traffic.rx", 158, "coin" => "KMD");

        mm_gauge!(metrics, "rpc.connection.count", 3, "coin" => "KMD");

        let end = sink.now();
        mm_timing!(metrics,
                   "rpc.query.spent_time",
                   start,
                   end,
                   "coin" => "KMD",
                   "method" => "blockchain.transaction.get");

        block_on(async { Timer::sleep(6.).await });

        mm_counter!(metrics, "rpc.traffic.tx", 30, "coin" => "BTC");
        mm_counter!(metrics, "rpc.traffic.rx", 44, "coin" => "BTC");

        mm_gauge!(metrics, "rpc.connection.count", 5, "coin" => "KMD");

        let end = sink.now();
        mm_timing!(metrics,
                   "rpc.query.spent_time",
                   start,
                   end,
                   "coin"=> "KMD",
                   "method"=>"blockchain.transaction.get");

        // measure without labels
        mm_counter!(metrics, "test.counter", 0);
        mm_gauge!(metrics, "test.gauge", 1);
        let end = sink.now();
        mm_timing!(metrics, "test.uptime", start, end);

        block_on(async { Timer::sleep(6.).await });
    }

    #[test]
    fn test_collect_json() {
        fn do_query(sink: &Sink, duration: f64) -> (u64, u64) {
            let start = sink.now();
            block_on(async { Timer::sleep(duration).await });
            let end = sink.now();
            (start, end)
        }

        fn record_to_hist(hist: &mut Histogram<u64>, start_end: (u64, u64)) {
            let delta = start_end.1.delta(start_end.0);
            hist.record(delta).unwrap()
        }

        let metrics_shared = MetricsArc::new();
        let metrics = metrics_shared.weak();

        metrics_shared.init().unwrap();
        let mut sink = metrics_shared.sink().unwrap();

        mm_counter!(metrics, "rpc.traffic.tx", 62, "coin" => "BTC");
        mm_counter!(metrics, "rpc.traffic.rx", 105, "coin" => "BTC");

        mm_counter!(metrics, "rpc.traffic.tx", 30, "coin" => "BTC");
        mm_counter!(metrics, "rpc.traffic.rx", 44, "coin" => "BTC");

        mm_counter!(metrics, "rpc.traffic.tx", 54, "coin" => "KMD");
        mm_counter!(metrics, "rpc.traffic.rx", 158, "coin" => "KMD");

        mm_gauge!(metrics, "rpc.connection.count", 3, "coin" => "KMD");

        // counter, gauge and timing may be collected also by sink API
        sink.update_gauge_with_labels("rpc.connection.count", 5, &[("coin", "KMD")]);

        let mut expected_hist = Histogram::new(3).unwrap();

        let query_time = do_query(&sink, 0.1);
        record_to_hist(&mut expected_hist, query_time);
        mm_timing!(metrics,
                   "rpc.query.spent_time",
                   query_time.0, // start
                   query_time.1, // end
                   "coin" => "KMD",
                   "method" => "blockchain.transaction.get");

        let query_time = do_query(&sink, 0.2);
        record_to_hist(&mut expected_hist, query_time);
        mm_timing!(metrics,
                   "rpc.query.spent_time",
                   query_time.0, // start
                   query_time.1, // end
                   "coin" => "KMD",
                   "method" => "blockchain.transaction.get");


        let expected = json!({
            "rpc": {
                "traffic": {
                    "tx{coin=\"BTC\"}": 92,
                    "rx{coin=\"BTC\"}": 149,
                    "tx{coin=\"KMD\"}": 54,
                    "rx{coin=\"KMD\"}": 158
                },
                "connection": {
                    "count{coin=\"KMD\"}": 5
                },
                "query": {
                    "spent_time{coin=\"KMD\",method=\"blockchain.transaction.get\"} count": 2,
                    "spent_time{coin=\"KMD\",method=\"blockchain.transaction.get\"} max": expected_hist.value_at_quantile(1.),
                    "spent_time{coin=\"KMD\",method=\"blockchain.transaction.get\"} min": expected_hist.value_at_quantile(0.)
                }
            }
        });

        let actual = metrics_shared.collect_json().unwrap();
        assert_eq!(actual, expected);
    }
}
