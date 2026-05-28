use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use arc_metrics::{IntCounter, IntGauge};
use tokio::sync::{Notify, RwLock, broadcast};

pub struct SequencedBroadcast<T> {
    state: Arc<State<T>>,
}

/// Sends sequence-numbered messages into a [`SequencedBroadcast`].
pub struct SequencedSender<T> {
    next_seq: u64,
    state: Arc<State<T>>,
}

/// Receives sequence-numbered messages from replay history and live broadcast.
///
/// A receiver may observe the same sequence in both its catch-up replay and the
/// live broadcast channel. This type treats those repeated sequences as
/// duplicates and skips them before returning the next expected message.
pub struct SequencedReceiver<T> {
    state: Arc<State<T>>,
    next_seq: u64,
    replay: VecDeque<SequencedItem<T>>,
    live_rx: broadcast::Receiver<SequencedItem<T>>,
    terminal: Option<SequencedRecvError>,
    active: bool,
}

#[derive(Default, Debug)]
pub struct SequencedBroadcastMetrics {
    pub oldest_sequence: IntGauge,
    pub next_sequence: IntGauge,
    pub new_client_drop_count: IntCounter,
    pub new_client_accept_count: IntCounter,
    pub active_subs_gauge: IntGauge,
    pub disconnect_count: IntCounter,
    pub duplicate_skip_count: IntCounter,
    pub lagged_receiver_count: IntCounter,
}

#[derive(Debug, Clone)]
pub struct SequencedBroadcastSettings {
    pub history_capacity: usize,
    pub broadcast_capacity: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettingsError {
    ZeroHistoryCapacity,
    ZeroBroadcastCapacity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscribeError {
    SequenceTooFarAhead { seq: u64, max: u64 },
    SequenceTooFarBehind { seq: u64, min: u64 },
    Closed,
}

#[derive(Debug, PartialEq, Eq)]
pub enum SequencedSenderError<T> {
    InvalidSequence { expected: u64, got: u64, item: T },
    Closed(T),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SequencedRecvError {
    Closed,
    Lagged {
        expected: u64,
        got: u64,
        skipped: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SequencedTryRecvError {
    Empty,
    Closed,
    Lagged {
        expected: u64,
        got: u64,
        skipped: u64,
    },
}

struct State<T> {
    live_tx: broadcast::Sender<SequencedItem<T>>,
    history: RwLock<History<T>>,
    closed: AtomicBool,
    close_notify: Notify,
    metrics: Arc<SequencedBroadcastMetrics>,
}

#[derive(Debug, Clone)]
struct SequencedItem<T> {
    seq: u64,
    item: T,
}

struct History<T> {
    oldest_seq: u64,
    next_seq: u64,
    entries: VecDeque<SequencedItem<T>>,
    capacity: usize,
}

impl Default for SequencedBroadcastSettings {
    fn default() -> Self {
        SequencedBroadcastSettings {
            history_capacity: 16 * 1024,
            broadcast_capacity: 16 * 1024,
        }
    }
}

impl<T> SequencedBroadcast<T>
where
    T: Send + Clone + 'static,
{
    pub fn new(
        next_seq: u64,
        settings: SequencedBroadcastSettings,
    ) -> Result<(Self, SequencedSender<T>), SettingsError> {
        if settings.history_capacity == 0 {
            return Err(SettingsError::ZeroHistoryCapacity);
        }

        if settings.broadcast_capacity == 0 {
            return Err(SettingsError::ZeroBroadcastCapacity);
        }

        let (live_tx, _) = broadcast::channel(settings.broadcast_capacity);
        let metrics = Arc::new(SequencedBroadcastMetrics {
            oldest_sequence: {
                let i = IntGauge::default();
                i.set(next_seq);
                i
            },
            next_sequence: {
                let i = IntGauge::default();
                i.set(next_seq);
                i
            },
            ..Default::default()
        });

        let state = Arc::new(State {
            live_tx,
            history: RwLock::new(History {
                oldest_seq: next_seq,
                next_seq,
                entries: VecDeque::with_capacity(settings.history_capacity),
                capacity: settings.history_capacity,
            }),
            closed: AtomicBool::new(false),
            close_notify: Notify::new(),
            metrics,
        });

        Ok((
            Self {
                state: state.clone(),
            },
            SequencedSender { next_seq, state },
        ))
    }

    pub async fn subscribe_from(
        &self,
        next_sequence: u64,
    ) -> Result<SequencedReceiver<T>, SubscribeError> {
        // Subscribe to live messages before copying history. That ordering can
        // duplicate messages across replay and live delivery, but it prevents a
        // missed sequence between the history snapshot and live subscription.
        let live_rx = self.state.live_tx.subscribe();
        let history = self.state.history.read().await;

        if next_sequence < history.oldest_seq {
            self.state.metrics.new_client_drop_count.inc();
            return Err(SubscribeError::SequenceTooFarBehind {
                seq: next_sequence,
                min: history.oldest_seq,
            });
        }

        if history.next_seq < next_sequence {
            self.state.metrics.new_client_drop_count.inc();
            return Err(SubscribeError::SequenceTooFarAhead {
                seq: next_sequence,
                max: history.next_seq,
            });
        }

        let replay = history
            .entries
            .iter()
            .filter(|entry| next_sequence <= entry.seq)
            .cloned()
            .collect();

        drop(history);

        self.state.metrics.new_client_accept_count.inc();
        self.state.metrics.active_subs_gauge.inc();

        Ok(SequencedReceiver {
            state: self.state.clone(),
            next_seq: next_sequence,
            replay,
            live_rx,
            terminal: None,
            active: true,
        })
    }

    pub fn metrics_ref(&self) -> &SequencedBroadcastMetrics {
        &self.state.metrics
    }

    pub fn metrics(&self) -> Arc<SequencedBroadcastMetrics> {
        self.state.metrics.clone()
    }

    pub fn is_closed(&self) -> bool {
        self.state.closed.load(Ordering::Acquire)
    }

    pub async fn closed(&self) {
        loop {
            let notified = self.state.close_notify.notified();
            if self.is_closed() {
                return;
            }

            notified.await;
        }
    }
}

impl<T> SequencedSender<T> {
    pub fn seq(&self) -> u64 {
        self.next_seq
    }

    pub fn is_closed(&self) -> bool {
        self.state.closed.load(Ordering::Acquire)
    }

    pub async fn closed(&self) {
        loop {
            let notified = self.state.close_notify.notified();
            if self.is_closed() {
                return;
            }

            notified.await;
        }
    }

    pub fn close(&mut self) {
        if !self.state.closed.swap(true, Ordering::AcqRel) {
            self.state.close_notify.notify_waiters();
        }
    }
}

impl<T> SequencedSender<T>
where
    T: Send + Clone + 'static,
{
    pub async fn send(&mut self, item: T) -> Result<u64, SequencedSenderError<T>> {
        self.send_at(self.next_seq, item).await
    }

    pub async fn send_at(&mut self, seq: u64, item: T) -> Result<u64, SequencedSenderError<T>> {
        if self.is_closed() {
            return Err(SequencedSenderError::Closed(item));
        }

        if seq != self.next_seq {
            return Err(SequencedSenderError::InvalidSequence {
                expected: self.next_seq,
                got: seq,
                item,
            });
        }

        let mut history = self.state.history.write().await;
        if self.is_closed() {
            return Err(SequencedSenderError::Closed(item));
        }

        /* note, do capacity check before push to ensure we don't allocate more memory */
        if history.capacity <= history.entries.len() {
            history.entries.pop_front();
            history.oldest_seq += 1;
        }

        let message = SequencedItem { seq, item };
        history.entries.push_back(message.clone());
        history.next_seq += 1;

        self.state.metrics.oldest_sequence.set(history.oldest_seq);
        self.state.metrics.next_sequence.set(history.next_seq);

        drop(history);

        let _ = self.state.live_tx.send(message);
        self.next_seq += 1;

        Ok(seq)
    }
}

impl<T> Drop for SequencedSender<T> {
    fn drop(&mut self) {
        self.close();
    }
}

impl<T> SequencedReceiver<T>
where
    T: Send + Clone + 'static,
{
    pub async fn recv(&mut self) -> Result<(u64, T), SequencedRecvError> {
        if let Some(error) = &self.terminal {
            return Err(error.clone());
        }

        loop {
            if let Some(item) = self.pop_replay()? {
                return Ok(item);
            }

            if self.state.closed.load(Ordering::Acquire) {
                match self.live_rx.try_recv() {
                    Ok(message) => match self.handle_message(message) {
                        Ok(Some(item)) => return Ok(item),
                        Ok(None) => continue,
                        Err(error) => return Err(error),
                    },
                    Err(broadcast::error::TryRecvError::Empty)
                    | Err(broadcast::error::TryRecvError::Closed) => {
                        return Err(self.terminate(SequencedRecvError::Closed));
                    }
                    Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                        return Err(self.terminate_lagged(skipped));
                    }
                }
            }

            tokio::select! {
                result = self.live_rx.recv() => {
                    match result {
                        Ok(message) => match self.handle_message(message) {
                            Ok(Some(item)) => return Ok(item),
                            Ok(None) => continue,
                            Err(error) => return Err(error),
                        },
                        Err(broadcast::error::RecvError::Closed) => {
                            return Err(self.terminate(SequencedRecvError::Closed));
                        }
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            return Err(self.terminate_lagged(skipped));
                        }
                    }
                }
                _ = self.state.close_notify.notified() => {
                    continue;
                }
            }
        }
    }

    pub fn try_recv(&mut self) -> Result<(u64, T), SequencedTryRecvError> {
        if let Some(error) = &self.terminal {
            return Err(error.clone().into());
        }

        loop {
            if let Some(item) = self.pop_replay()? {
                return Ok(item);
            }

            match self.live_rx.try_recv() {
                Ok(message) => match self
                    .handle_message(message)
                    .map_err(SequencedTryRecvError::from)?
                {
                    Some(item) => return Ok(item),
                    None => continue,
                },
                Err(broadcast::error::TryRecvError::Empty) => {
                    if self.state.closed.load(Ordering::Acquire) {
                        return Err(SequencedTryRecvError::from(
                            self.terminate(SequencedRecvError::Closed),
                        ));
                    }

                    return Err(SequencedTryRecvError::Empty);
                }
                Err(broadcast::error::TryRecvError::Closed) => {
                    return Err(SequencedTryRecvError::from(
                        self.terminate(SequencedRecvError::Closed),
                    ));
                }
                Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                    return Err(SequencedTryRecvError::from(self.terminate_lagged(skipped)));
                }
            }
        }
    }

    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    pub fn is_closed(&self) -> bool {
        self.terminal.is_some() || self.state.closed.load(Ordering::Acquire)
    }

    fn pop_replay(&mut self) -> Result<Option<(u64, T)>, SequencedRecvError> {
        while let Some(message) = self.replay.pop_front() {
            match self.handle_message(message)? {
                Some(item) => return Ok(Some(item)),
                None => continue,
            }
        }

        Ok(None)
    }

    fn handle_message(
        &mut self,
        message: SequencedItem<T>,
    ) -> Result<Option<(u64, T)>, SequencedRecvError> {
        if message.seq < self.next_seq {
            self.state.metrics.duplicate_skip_count.inc();
            return Ok(None);
        }

        if self.next_seq < message.seq {
            let expected = self.next_seq;
            let skipped = message.seq - expected;
            return Err(self.terminate(SequencedRecvError::Lagged {
                expected,
                got: message.seq,
                skipped,
            }));
        }

        self.next_seq = message.seq + 1;
        Ok(Some((message.seq, message.item)))
    }

    fn terminate_lagged(&mut self, skipped: u64) -> SequencedRecvError {
        let expected = self.next_seq;
        self.terminate(SequencedRecvError::Lagged {
            expected,
            got: expected.saturating_add(skipped),
            skipped,
        })
    }

    fn terminate(&mut self, error: SequencedRecvError) -> SequencedRecvError {
        if self.terminal.is_none() {
            if self.active {
                self.active = false;
                self.state.metrics.active_subs_gauge.dec();
                self.state.metrics.disconnect_count.inc();
            }

            if matches!(error, SequencedRecvError::Lagged { .. }) {
                self.state.metrics.lagged_receiver_count.inc();
            }

            self.terminal = Some(error.clone());
        }

        error
    }
}

impl<T> Drop for SequencedReceiver<T> {
    fn drop(&mut self) {
        if self.active {
            self.active = false;
            self.state.metrics.active_subs_gauge.dec();
        }
    }
}

impl From<SequencedRecvError> for SequencedTryRecvError {
    fn from(value: SequencedRecvError) -> Self {
        match value {
            SequencedRecvError::Closed => SequencedTryRecvError::Closed,
            SequencedRecvError::Lagged {
                expected,
                got,
                skipped,
            } => SequencedTryRecvError::Lagged {
                expected,
                got,
                skipped,
            },
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio::{
        task::JoinHandle,
        time::{Duration, Instant, sleep, timeout},
    };

    fn settings(history_capacity: usize, broadcast_capacity: usize) -> SequencedBroadcastSettings {
        SequencedBroadcastSettings {
            history_capacity,
            broadcast_capacity,
        }
    }

    #[tokio::test]
    async fn basic_live_delivery() {
        let (subs, mut tx) = SequencedBroadcast::new(10, SequencedBroadcastSettings::default())
            .expect("valid settings");
        let mut rx = subs.subscribe_from(10).await.unwrap();

        assert_eq!(tx.send("a").await.unwrap(), 10);
        assert_eq!(tx.send("b").await.unwrap(), 11);
        assert_eq!(tx.send("c").await.unwrap(), 12);

        assert_eq!(rx.recv().await.unwrap(), (10, "a"));
        assert_eq!(rx.recv().await.unwrap(), (11, "b"));
        assert_eq!(rx.recv().await.unwrap(), (12, "c"));
    }

    #[tokio::test]
    async fn history_catchup_delivery() {
        let (subs, mut tx) = SequencedBroadcast::new(0, SequencedBroadcastSettings::default())
            .expect("valid settings");

        tx.send("a").await.unwrap();
        tx.send("b").await.unwrap();

        let mut rx = subs.subscribe_from(0).await.unwrap();
        assert_eq!(rx.recv().await.unwrap(), (0, "a"));
        assert_eq!(rx.recv().await.unwrap(), (1, "b"));

        tx.send("c").await.unwrap();
        assert_eq!(rx.recv().await.unwrap(), (2, "c"));
    }

    #[tokio::test]
    async fn subscribe_from_middle_of_history() {
        let (subs, mut tx) = SequencedBroadcast::new(0, SequencedBroadcastSettings::default())
            .expect("valid settings");

        tx.send("a").await.unwrap();
        tx.send("b").await.unwrap();
        tx.send("c").await.unwrap();

        let mut rx = subs.subscribe_from(1).await.unwrap();
        assert_eq!(rx.recv().await.unwrap(), (1, "b"));
        assert_eq!(rx.recv().await.unwrap(), (2, "c"));
        assert_eq!(rx.try_recv(), Err(SequencedTryRecvError::Empty));
    }

    #[tokio::test]
    async fn reject_too_far_behind() {
        let (subs, mut tx) = SequencedBroadcast::new(0, settings(2, 16)).expect("valid settings");

        tx.send("a").await.unwrap();
        tx.send("b").await.unwrap();
        tx.send("c").await.unwrap();

        let error = match subs.subscribe_from(0).await {
            Ok(_) => panic!("expected subscribe error"),
            Err(error) => error,
        };
        assert_eq!(
            error,
            SubscribeError::SequenceTooFarBehind { seq: 0, min: 1 }
        );
        assert_eq!(subs.metrics_ref().new_client_drop_count.load(), 1);
    }

    #[tokio::test]
    async fn reject_too_far_ahead() {
        let (subs, mut tx) = SequencedBroadcast::new(0, SequencedBroadcastSettings::default())
            .expect("valid settings");

        tx.send("a").await.unwrap();
        tx.send("b").await.unwrap();
        tx.send("c").await.unwrap();

        let error = match subs.subscribe_from(4).await {
            Ok(_) => panic!("expected subscribe error"),
            Err(error) => error,
        };
        assert_eq!(
            error,
            SubscribeError::SequenceTooFarAhead { seq: 4, max: 3 }
        );
        assert_eq!(subs.metrics_ref().new_client_drop_count.load(), 1);
    }

    #[tokio::test]
    async fn duplicate_live_messages_are_ignored() {
        let (subs, mut tx) = SequencedBroadcast::new(0, SequencedBroadcastSettings::default())
            .expect("valid settings");
        let mut rx = subs.subscribe_from(0).await.unwrap();

        rx.replay.push_back(SequencedItem { seq: 0, item: "a" });

        tx.send("a").await.unwrap();
        tx.send("b").await.unwrap();

        assert_eq!(rx.recv().await.unwrap(), (0, "a"));
        assert_eq!(rx.recv().await.unwrap(), (1, "b"));
        assert_eq!(subs.metrics_ref().duplicate_skip_count.load(), 1);
    }

    #[tokio::test]
    async fn broadcast_lag_returns_error() {
        let (subs, mut tx) = SequencedBroadcast::new(0, settings(16, 2)).expect("valid settings");
        let mut rx = subs.subscribe_from(0).await.unwrap();

        for i in 0..8 {
            tx.send(i).await.unwrap();
        }

        assert!(matches!(
            rx.recv().await,
            Err(SequencedRecvError::Lagged { .. })
        ));
        assert_eq!(subs.metrics_ref().active_subs_gauge.load(), 0);
        assert_eq!(subs.metrics_ref().disconnect_count.load(), 1);
        assert_eq!(subs.metrics_ref().lagged_receiver_count.load(), 1);
    }

    #[tokio::test]
    async fn gap_returns_lagged_error() {
        let (subs, _tx) = SequencedBroadcast::new(5, SequencedBroadcastSettings::default())
            .expect("valid settings");
        let mut rx = subs.subscribe_from(5).await.unwrap();

        let _ = subs.state.live_tx.send(SequencedItem {
            seq: 7,
            item: "gap",
        });

        assert_eq!(
            rx.recv().await.unwrap_err(),
            SequencedRecvError::Lagged {
                expected: 5,
                got: 7,
                skipped: 2,
            }
        );
    }

    #[tokio::test]
    async fn send_succeeds_without_receivers() {
        let (subs, mut tx) = SequencedBroadcast::new(0, SequencedBroadcastSettings::default())
            .expect("valid settings");

        assert_eq!(tx.send("a").await.unwrap(), 0);
        assert_eq!(tx.send("b").await.unwrap(), 1);

        let mut rx = subs.subscribe_from(0).await.unwrap();
        assert_eq!(rx.recv().await.unwrap(), (0, "a"));
        assert_eq!(rx.recv().await.unwrap(), (1, "b"));
    }

    #[tokio::test]
    async fn sender_close_closes_receivers_after_replay() {
        let (subs, mut tx) = SequencedBroadcast::new(0, SequencedBroadcastSettings::default())
            .expect("valid settings");

        tx.send("a").await.unwrap();
        let mut rx = subs.subscribe_from(0).await.unwrap();
        tx.close();

        assert_eq!(rx.recv().await.unwrap(), (0, "a"));
        assert_eq!(rx.recv().await.unwrap_err(), SequencedRecvError::Closed);
    }

    #[tokio::test]
    async fn subscribe_after_close_can_replay_history() {
        let (subs, mut tx) = SequencedBroadcast::new(0, SequencedBroadcastSettings::default())
            .expect("valid settings");

        tx.send("a").await.unwrap();
        tx.send("b").await.unwrap();
        tx.close();

        let mut rx = subs.subscribe_from(0).await.unwrap();
        assert_eq!(rx.recv().await.unwrap(), (0, "a"));
        assert_eq!(rx.recv().await.unwrap(), (1, "b"));
        assert_eq!(rx.recv().await.unwrap_err(), SequencedRecvError::Closed);
    }

    #[tokio::test]
    async fn send_after_close_returns_item() {
        let (_subs, mut tx) = SequencedBroadcast::new(0, SequencedBroadcastSettings::default())
            .expect("valid settings");

        tx.close();

        assert_eq!(
            tx.send("a").await.unwrap_err(),
            SequencedSenderError::Closed("a")
        );
    }

    #[tokio::test]
    async fn send_at_validates_sequence() {
        let (_subs, mut tx) = SequencedBroadcast::new(10, SequencedBroadcastSettings::default())
            .expect("valid settings");

        assert_eq!(
            tx.send_at(11, "a").await.unwrap_err(),
            SequencedSenderError::InvalidSequence {
                expected: 10,
                got: 11,
                item: "a",
            }
        );
        assert_eq!(tx.seq(), 10);
    }

    #[tokio::test]
    async fn try_recv_empty() {
        let (subs, _tx) =
            SequencedBroadcast::<&'static str>::new(0, SequencedBroadcastSettings::default())
                .expect("valid settings");
        let mut rx = subs.subscribe_from(0).await.unwrap();

        assert_eq!(rx.try_recv(), Err(SequencedTryRecvError::Empty));
    }

    #[tokio::test]
    async fn active_metrics_decrement_on_drop() {
        let (subs, _tx) =
            SequencedBroadcast::<&'static str>::new(0, SequencedBroadcastSettings::default())
                .expect("valid settings");
        let rx_1 = subs.subscribe_from(0).await.unwrap();
        let _rx_2 = subs.subscribe_from(0).await.unwrap();

        assert_eq!(subs.metrics_ref().active_subs_gauge.load(), 2);
        drop(rx_1);
        assert_eq!(subs.metrics_ref().active_subs_gauge.load(), 1);
    }

    #[tokio::test]
    async fn closed_waits_for_sender_close() {
        let (subs, mut tx) =
            SequencedBroadcast::<&'static str>::new(0, SequencedBroadcastSettings::default())
                .expect("valid settings");

        assert!(
            timeout(Duration::from_millis(10), subs.closed())
                .await
                .is_err()
        );
        tx.close();
        timeout(Duration::from_millis(10), subs.closed())
            .await
            .expect("closed should resolve");
    }

    #[tokio::test]
    async fn settings_validation() {
        let err = match SequencedBroadcast::<()>::new(
            0,
            SequencedBroadcastSettings {
                history_capacity: 0,
                broadcast_capacity: 1,
            },
        ) {
            Ok(_) => panic!("expected settings error"),
            Err(error) => error,
        };
        assert_eq!(err, SettingsError::ZeroHistoryCapacity);

        let err = match SequencedBroadcast::<()>::new(
            0,
            SequencedBroadcastSettings {
                history_capacity: 1,
                broadcast_capacity: 0,
            },
        ) {
            Ok(_) => panic!("expected settings error"),
            Err(error) => error,
        };
        assert_eq!(err, SettingsError::ZeroBroadcastCapacity);
    }

    #[derive(Clone)]
    struct FuzzRng {
        state: u64,
    }

    impl FuzzRng {
        fn new(seed: u64) -> Self {
            Self { state: seed }
        }

        fn next(&mut self) -> u64 {
            self.state = self
                .state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            self.state
        }

        fn range(&mut self, upper: usize) -> usize {
            if upper == 0 {
                return 0;
            }

            (self.next() as usize) % upper
        }

        fn one_in(&mut self, denominator: usize) -> bool {
            self.range(denominator) == 0
        }
    }

    async fn run_fuzz_receiver(mut rx: SequencedReceiver<u64>, mut rng: FuzzRng) -> u64 {
        let mut received = 0;

        loop {
            if rng.one_in(3) {
                let expected = rx.next_seq();
                match rx.try_recv() {
                    Ok((seq, item)) => {
                        assert_eq!(seq, expected);
                        assert_eq!(item, seq);
                        received += 1;
                    }
                    Err(SequencedTryRecvError::Empty) => {
                        sleep(Duration::from_micros((rng.range(500) + 1) as u64)).await;
                    }
                    Err(SequencedTryRecvError::Closed)
                    | Err(SequencedTryRecvError::Lagged { .. }) => break,
                }
            } else {
                let expected = rx.next_seq();
                match timeout(Duration::from_millis(50), rx.recv()).await {
                    Ok(Ok((seq, item))) => {
                        assert_eq!(seq, expected);
                        assert_eq!(item, seq);
                        received += 1;
                    }
                    Ok(Err(SequencedRecvError::Closed))
                    | Ok(Err(SequencedRecvError::Lagged { .. })) => break,
                    Err(_) => {}
                }
            }

            if received != 0 && rng.one_in(128) {
                break;
            }

            if rng.one_in(8) {
                sleep(Duration::from_micros((rng.range(1_000) + 1) as u64)).await;
            }
        }

        received
    }

    async fn join_finished(tasks: &mut Vec<JoinHandle<u64>>) -> u64 {
        let mut received = 0;

        while let Some(pos) = tasks.iter().position(JoinHandle::is_finished) {
            received += tasks
                .swap_remove(pos)
                .await
                .expect("fuzz receiver task panicked");
        }

        received
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "30 second fuzzy stability test; run with `cargo test fuzzy_stability_30_seconds -- --ignored`"]
    async fn fuzzy_stability_30_seconds() {
        let deadline = Instant::now() + Duration::from_secs(30);
        let (subs, mut tx) = SequencedBroadcast::new(0, settings(512, 64)).expect("valid settings");
        let sender_deadline = deadline;

        let sender = tokio::spawn(async move {
            let mut rng = FuzzRng::new(0xa511_ce5d_f00d);
            let mut sent = 0;

            while Instant::now() < sender_deadline {
                let seq = tx.seq();
                let sent_seq = tx
                    .send(seq)
                    .await
                    .expect("send should succeed before close");
                assert_eq!(sent_seq, seq);
                sent += 1;

                if rng.one_in(4) {
                    tokio::task::yield_now().await;
                }

                if rng.one_in(16) {
                    sleep(Duration::from_micros((rng.range(250) + 1) as u64)).await;
                }
            }

            tx.close();
            sent
        });

        let mut rng = FuzzRng::new(0x5eed_5eed_cafe);
        let mut tasks: Vec<JoinHandle<u64>> = Vec::new();
        let mut total_received = 0;

        while Instant::now() < deadline {
            total_received += join_finished(&mut tasks).await;

            if tasks.len() < 128 {
                let history = subs.state.history.read().await;
                let oldest = history.oldest_seq;
                let next = history.next_seq;
                drop(history);

                let valid_span = next.saturating_sub(oldest) + 1;
                let seq = oldest + rng.range(valid_span as usize) as u64;

                match subs.subscribe_from(seq).await {
                    Ok(rx) => {
                        let seed = rng.next();
                        tasks.push(tokio::spawn(run_fuzz_receiver(rx, FuzzRng::new(seed))));
                    }
                    Err(SubscribeError::SequenceTooFarBehind { .. })
                    | Err(SubscribeError::SequenceTooFarAhead { .. }) => {}
                    Err(SubscribeError::Closed) => break,
                }
            }

            if rng.one_in(4) {
                let history = subs.state.history.read().await;
                let too_old = history.oldest_seq.saturating_sub(1);
                let too_new = history.next_seq.saturating_add(1_000);
                let oldest = history.oldest_seq;
                drop(history);

                if oldest != 0 {
                    assert!(matches!(
                        subs.subscribe_from(too_old).await,
                        Err(SubscribeError::SequenceTooFarBehind { .. })
                    ));
                }

                assert!(matches!(
                    subs.subscribe_from(too_new).await,
                    Err(SubscribeError::SequenceTooFarAhead { .. })
                ));
            }

            sleep(Duration::from_millis((rng.range(5) + 1) as u64)).await;
        }

        let sent = sender.await.expect("fuzz sender task panicked");

        for task in tasks {
            total_received += task.await.expect("fuzz receiver task panicked");
        }

        assert!(sent > 1_000);
        assert!(total_received > 0);
        assert_eq!(subs.metrics_ref().next_sequence.load(), sent);
        assert_eq!(subs.metrics_ref().active_subs_gauge.load(), 0);
    }
}
