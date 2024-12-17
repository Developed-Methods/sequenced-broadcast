use std::{
    collections::VecDeque,
    fmt::Debug,
    future::{poll_fn, Future},
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::Poll,
    time::{Duration, Instant},
};

use tokio::sync::{
    mpsc::{channel, error::{SendError, TryRecvError, TrySendError}, Permit, Receiver, Sender},
    oneshot,
};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

pub struct SequencedBroadcast<T> {
    new_subscriber: Sender<NewClient<T>>,
    metrics: Arc<SequencedBroadcastMetrics>,
    shutdown: CancellationToken,
    next_req: Sender<NextReq<T>>,
}

struct NextReq<T> {
    start_seq: u64,
    handler: oneshot::Sender<SequencedSender<T>>,
}

pub struct SequencedSender<T> {
    next_seq: u64,
    send: Sender<(u64, T)>,
}

pub struct SequencedReceiver<T> {
    next_seq: u64,
    receiver: Receiver<(u64, T)>,
}

#[derive(Debug, Default)]
pub struct SequencedBroadcastMetrics {
    pub oldest_sequence: AtomicU64,
    pub next_sequence: AtomicU64,
    pub new_client_drop_count: AtomicU64,
    pub new_client_accept_count: AtomicU64,
    pub lagging_subs_gauge: AtomicU64,
    pub active_subs_gauge: AtomicU64,
    pub min_sub_sequence_gauge: AtomicU64,
    pub disconnect_count: AtomicU64,
}

struct Subscriber<T> {
    id: u64,
    next_sequence: u64,
    tx: Sender<(u64, T)>,
    allow_drop: bool,
    lag_started_at: Option<Instant>,
    pending: Option<T>,
}

#[derive(Debug, Clone)]
pub struct SequencedBroadcastSettings {
    pub subscriber_channel_len: usize,
    pub lag_start_threshold: u64,
    pub lag_end_threshold: u64,
    pub max_time_lag: Duration,
    pub min_history: u64,
}

struct Worker<T> {
    rx: Receiver<(u64, T)>,
    next_sub_id: u64,
    new_client: Receiver<NewClient<T>>,
    next_client: Option<NewClient<T>>,
    subscribers: Vec<Subscriber<T>>,
    queue: VecDeque<(u64, T)>,
    next_queue_seq: u64,
    metrics: Arc<SequencedBroadcastMetrics>,
    settings: SequencedBroadcastSettings,
    shutdown: CancellationToken,
    next_req_rx: Receiver<NextReq<T>>,
    next_req: Option<NextReq<T>>,
}

impl Default for SequencedBroadcastSettings {
    fn default() -> Self {
        SequencedBroadcastSettings {
            subscriber_channel_len: 1024,
            lag_start_threshold: 1024 * 8,
            lag_end_threshold: 1024 * 4,
            max_time_lag: Duration::from_secs(2),
            min_history: 2048,
        }
    }
}

impl<T> SequencedSender<T> {
    pub fn new(next_seq: u64, send: Sender<(u64, T)>) -> Self {
        SequencedSender { next_seq, send }
    }

    pub fn is_closed(&self) -> bool {
        self.send.is_closed()
    }

    pub async fn closed(&self) {
        self.send.closed().await
    }

    pub async fn safe_send(&mut self, seq: u64, item: T) -> Result<(), SequencedSenderError<T>> {
        self._send(Some(seq), item).await
    }

    pub async fn send(&mut self, item: T) -> Result<(), SequencedSenderError<T>> {
        self._send(None, item).await
    }

    pub fn try_send(&mut self, item: T) -> Result<(), TrySendError<T>> {
        match self.send.try_send((self.next_seq, item)) {
            Ok(()) => {
                self.next_seq += 1;
                Ok(())
            }
            Err(TrySendError::Full(err)) => Err(TrySendError::Full(err.1)),
            Err(TrySendError::Closed(err)) => Err(TrySendError::Closed(err.1)),
        }
    }

    pub async fn reserve(&mut self) -> Result<SequencedSenderPermit<T>, SendError<()>> {
        let permit = self.send.reserve().await?;

        Ok(SequencedSenderPermit {
            next_seq: &mut self.next_seq,
            permit,
        })
    }

    async fn _send(&mut self, seq: Option<u64>, item: T) -> Result<(), SequencedSenderError<T>> {
        if let Some(seq) = seq {
            if seq != self.next_seq {
                return Err(SequencedSenderError::InvalidSequence(self.next_seq, item));
            }
        }

        if let Err(error) = self.send.send((self.next_seq, item)).await {
            return Err(SequencedSenderError::ChannelClosed(error.0.1));
        }

        self.next_seq += 1;
        Ok(())
    }

    pub fn seq(&self) -> u64 {
        self.next_seq
    }
}

pub struct SequencedSenderPermit<'a, T> {
    next_seq: &'a mut u64,
    permit: Permit<'a, (u64, T)>,
}

impl<'a, T> SequencedSenderPermit<'a, T> {
    pub fn send(self, item: T) {
        let seq = *self.next_seq;
        self.permit.send((seq, item));
        *self.next_seq = seq + 1;
    }
}

impl<T> SequencedReceiver<T> {
    pub fn new(next_seq: u64, receiver: Receiver<(u64, T)>) -> Self {
        SequencedReceiver {
            next_seq,
            receiver
        }
    }

    pub fn is_closed(&self) -> bool {
        self.receiver.is_closed()
    }

    pub async fn recv(&mut self) -> Option<(u64, T)> {
        let (seq, action) = self.receiver.recv().await?;
        if self.next_seq != seq {
            panic!("expected sequence: {} but got: {}", self.next_seq, seq);
        }
        self.next_seq += 1;
        Some((seq, action))
    }

    pub fn try_recv(&mut self) -> Result<(u64, T), TryRecvError> {
        match self.receiver.try_recv() {
            Ok((seq, action)) => {
                if self.next_seq != seq {
                    panic!("expected sequence: {} but got: {}", self.next_seq, seq);
                }
                self.next_seq += 1;
                Ok((seq, action))
            }
            Err(error) => Err(error)
        }
    }

    pub fn unbundle(self) -> (u64, Receiver<(u64, T)>) {
        (self.next_seq, self.receiver)
    }

    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum SequencedSenderError<T> {
    InvalidSequence(u64, T),
    ChannelClosed(T),
}

impl<T> SequencedSenderError<T> {
    pub fn into_inner(self) -> T {
        match self {
            Self::InvalidSequence(_, v) => v,
            Self::ChannelClosed(v) => v,
        }
    }
}

impl<T: Send + Clone + 'static> SequencedBroadcast<T> {
    pub fn new(next_seq: u64, settings: SequencedBroadcastSettings) -> (Self, SequencedSender<T>) {
        let queue_cap = ((settings.lag_start_threshold as usize)
            .next_power_of_two()
            .max(1024)
            * 2)
        .max((settings.min_history as usize).next_power_of_two());

        assert!(settings.lag_end_threshold <= settings.lag_start_threshold);

        let (tx, rx) = channel(1024);
        let (client_tx, client_rx) = channel(32);

        let metrics = Arc::new(SequencedBroadcastMetrics {
            oldest_sequence: AtomicU64::new(next_seq),
            next_sequence: AtomicU64::new(next_seq),
            ..Default::default()
        });

        let shutdown = CancellationToken::new();
        let current_span = tracing::Span::current();

        let (next_req_tx, next_req_rx) = channel(1);

        tokio::spawn(
            Worker {
                rx,
                next_sub_id: 1,
                new_client: client_rx,
                next_client: None,
                subscribers: Vec::with_capacity(32),
                queue: VecDeque::with_capacity(queue_cap),
                next_queue_seq: next_seq,
                metrics: metrics.clone(),
                settings,
                shutdown: shutdown.clone(),
                next_req_rx,
                next_req: None,
            }
            .start()
            .instrument(current_span),
        );

        let tx = SequencedSender { next_seq, send: tx };

        (
            SequencedBroadcast {
                next_req: next_req_tx,
                new_subscriber: client_tx,
                metrics,
                shutdown,
            },
            tx,
        )
    }

    pub async fn add_client(
        &self,
        next_sequence: u64,
        allow_drop: bool,
    ) -> Option<SequencedReceiver<T>> {
        let (tx, rx) = oneshot::channel();

        self.new_subscriber
            .send(NewClient {
                response: tx,
                allow_drop,
                next_sequence,
            })
            .await
            .expect("Failed to queue new subscriber, worker crashed");

        rx.await.ok()
    }

    pub async fn replace_sender(&self, start_seq: u64) -> Option<SequencedSender<T>> {
        let (tx, rx) = oneshot::channel();

        self.next_req
            .send(NextReq {
                start_seq,
                handler: tx,
            })
            .await
            .expect("Worker shutdown");

        rx.await.ok()
    }

    pub fn metrics_ref(&self) -> &SequencedBroadcastMetrics {
        &self.metrics
    }

    pub fn metrics(&self) -> Arc<SequencedBroadcastMetrics> {
        self.metrics.clone()
    }

    pub fn shutdown(self) {
        self.shutdown.cancel();
    }
}

struct NewClient<T> {
    response: oneshot::Sender<SequencedReceiver<T>>,
    next_sequence: u64,
    allow_drop: bool,
}

impl<T> Debug for NewClient<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "NewClient {{ next_sequence: {}, allow_drop: {} }}",
            self.next_sequence, self.allow_drop
        )
    }
}

struct PendingSubSend<'a, T, F: 'a> {
    next: &'a mut Option<T>,
    next_sequence: &'a mut u64,
    reserve: F,
}

impl<T: Send + Clone + 'static> Worker<T> {
    async fn start(mut self) {
        let mut blocked_senders_idx = Vec::with_capacity(128);

        loop {
            tokio::task::yield_now().await;

            if self.next_client.is_none() {
                self.next_client = self.new_client.try_recv().ok();
            }

            if self.shutdown.is_cancelled() {
                tracing::info!("Stopping worker due to shutdown");
                break;
            }

            /* not possible to have more clients, kill worker */
            if self.subscribers.is_empty()
                && self.next_client.is_none()
                && self.new_client.is_closed()
            {
                tracing::info!("Stopping worker, rx closed and no more subscribers");
                break;
            }

            /* accept new clients */
            {
                let mut max_per_loop = 32;
                let min_allowed_seq = self
                    .queue
                    .front()
                    .map(|i| i.0)
                    .unwrap_or(self.next_queue_seq);

                if self.next_client.is_none() {
                    self.next_client = self.new_client.try_recv().ok();
                }

                while let Some(new) = self.next_client.take() {
                    /* Sequence in valid range */
                    if new.next_sequence < min_allowed_seq
                        || self.next_queue_seq < new.next_sequence
                    {
                        if new.next_sequence < min_allowed_seq {
                            tracing::info!(
                                "Subscriber rejected, seq({}) < min_allowed({})",
                                new.next_sequence,
                                min_allowed_seq
                            );
                        } else {
                            tracing::info!(
                                "Subscriber rejected, max_seq({}) < seq({})",
                                self.next_queue_seq,
                                new.next_sequence
                            );
                        }

                        self.metrics
                            .new_client_drop_count
                            .fetch_add(1, Ordering::Relaxed);
                        drop(new);

                        continue;
                    }

                    self.metrics
                        .new_client_accept_count
                        .fetch_add(1, Ordering::Relaxed);

                    /* Send Receiver to subscribers */
                    let (tx, rx) = channel(self.settings.subscriber_channel_len);
                    let rx = SequencedReceiver::<T> {
                        receiver: rx,
                        next_seq: new.next_sequence,
                    };

                    if new.response.send(rx).is_ok() {
                        let sub_id = self.next_sub_id;
                        self.next_sub_id += 1;

                        tracing::info!(
                            "Subscriber({}): Added, allow_drop: {}, next_sequence: {}, min_allowed_seq: {}",
                            sub_id, new.allow_drop, new.next_sequence, min_allowed_seq,
                        );

                        self.subscribers.push(Subscriber {
                            id: sub_id,
                            allow_drop: new.allow_drop,
                            next_sequence: new.next_sequence,
                            pending: None,
                            tx,
                            lag_started_at: None,
                        });
                    } else {
                        tracing::warn!("New subscriber accepted but receiver dropped");
                    }

                    /* ensure we don't block getting new clients */
                    if max_per_loop == 0 {
                        break;
                    }

                    max_per_loop -= 1;
                    self.next_client = self.new_client.try_recv().ok();
                }
            }

            if self.next_req.is_none() {
                self.next_req = self.next_req_rx.try_recv().ok();
            }

            /* fill queue with available data from rx */
            {
                let mut remaining_msg_count = (self.queue.capacity() - self.queue.len()).min(1024);

                loop {
                    /* see if we need to replace the receiver */
                    if matches!(&self.next_req, Some(next) if next.start_seq <= self.next_queue_seq)
                    {
                        let next = self.next_req.take().unwrap();
                        self.next_req = self.next_req_rx.try_recv().ok();

                        let (tx, rx) = channel(self.rx.capacity());

                        let sender = SequencedSender {
                            next_seq: self.next_queue_seq,
                            send: tx,
                        };

                        if next.handler.send(sender).is_ok() {
                            self.rx = rx;
                        }
                    }

                    if remaining_msg_count == 0 {
                        break;
                    }

                    let Ok((seq, item)) = self.rx.try_recv() else {
                        break;
                    };
                    assert_eq!(seq, self.next_queue_seq, "sequence is invalid");

                    self.queue.push_back((seq, item));
                    self.next_queue_seq += 1;

                    remaining_msg_count -= 1;
                }
            }

            self.metrics
                .next_sequence
                .store(self.next_queue_seq, Ordering::Relaxed);

            let oldest_queue_sequence = self
                .queue
                .front()
                .map(|v| v.0)
                .unwrap_or(self.next_queue_seq);

            let mut min_sub_sequence = self.next_queue_seq;
            blocked_senders_idx.clear();

            let mut i = 0;
            'next_sub: while i < self.subscribers.len() {
                let sub = &mut self.subscribers[i];

                'write_to_sub: {
                    /* try to send pending */
                    if let Some(next) = sub.pending.take() {
                        if let Err(error) = sub.tx.try_send((sub.next_sequence, next)) {
                            if matches!(error, TrySendError::Closed(_)) {
                                tracing::info!("Subscriber({}): channel closed, dropping", sub.id);

                                if sub.lag_started_at.is_some() {
                                    self.metrics
                                        .lagging_subs_gauge
                                        .fetch_sub(1, Ordering::Relaxed);
                                }

                                self.metrics
                                    .disconnect_count
                                    .fetch_add(1, Ordering::Relaxed);
                                self.subscribers.swap_remove(i);
                                continue 'next_sub;
                            }

                            sub.pending = Some(error.into_inner().1);
                            break 'write_to_sub;
                        }

                        sub.next_sequence += 1;
                    }

                    assert!(sub.pending.is_none());

                    /* sub is too far behind or is closed */
                    if sub.next_sequence < oldest_queue_sequence || sub.tx.is_closed() {
                        if sub.next_sequence < oldest_queue_sequence {
                            tracing::warn!(
                                "Subscriber({}): lag behind available data ({} < {}), dropping",
                                sub.id,
                                sub.next_sequence,
                                oldest_queue_sequence
                            );
                        } else {
                            tracing::info!("Subscriber({}): channel closed, dropping", sub.id);
                        }

                        if sub.lag_started_at.is_some() {
                            self.metrics
                                .lagging_subs_gauge
                                .fetch_sub(1, Ordering::Relaxed);
                        }

                        self.metrics
                            .disconnect_count
                            .fetch_add(1, Ordering::Relaxed);
                        self.subscribers.swap_remove(i);
                        continue 'next_sub;
                    }

                    let mut offset = (sub.next_sequence - oldest_queue_sequence) as usize;
                    assert!(
                        sub.next_sequence <= self.next_queue_seq,
                        "sub cannot be ahead of queue sequence"
                    );
                    assert!(
                        offset <= self.queue.len(),
                        "sub cannot be ahead of queue sequence"
                    );

                    /* sub is caught up */
                    if self.queue.len() == offset {
                        /* no more data available, close sub */
                        if self.rx.is_closed() {
                            tracing::info!(
                                "Subscriber({}): read all available data, dropping",
                                sub.id
                            );

                            if sub.lag_started_at.is_some() {
                                self.metrics
                                    .lagging_subs_gauge
                                    .fetch_sub(1, Ordering::Relaxed);
                            }

                            self.metrics
                                .disconnect_count
                                .fetch_add(1, Ordering::Relaxed);
                            self.subscribers.swap_remove(i);
                            continue 'next_sub;
                        }

                        break 'write_to_sub;
                    }

                    while offset < self.queue.len() {
                        let (seq, item) = self.queue.get(offset).unwrap();
                        assert_eq!(*seq, sub.next_sequence);

                        if let Err(error) = sub.tx.try_send((*seq, item.clone())) {
                            if matches!(error, TrySendError::Closed(_)) {
                                tracing::info!("Subscriber({}): channel closed, dropping", sub.id);

                                if sub.lag_started_at.is_some() {
                                    self.metrics
                                        .lagging_subs_gauge
                                        .fetch_sub(1, Ordering::Relaxed);
                                }
                                self.metrics
                                    .disconnect_count
                                    .fetch_add(1, Ordering::Relaxed);
                                self.subscribers.swap_remove(i);
                                continue 'next_sub;
                            }

                            sub.pending = Some(error.into_inner().1);
                            break 'write_to_sub;
                        }

                        sub.next_sequence += 1;
                        offset += 1;
                    }
                };

                'lag_drop: {
                    let lag = self.next_queue_seq - sub.next_sequence;

                    if lag <= self.settings.lag_end_threshold {
                        if let Some(lag_start) = sub.lag_started_at.take() {
                            tracing::info!(
                                "Subscriber({}): caught up after {:?}",
                                sub.id,
                                lag_start.elapsed()
                            );
                            self.metrics
                                .lagging_subs_gauge
                                .fetch_sub(1, Ordering::Relaxed);
                        }
                        break 'lag_drop;
                    }

                    /* note: allows subscriber to stay longer than max delay if under start threshold */

                    if lag < self.settings.lag_start_threshold {
                        break 'lag_drop;
                    }

                    let lag_duration = if let Some(lag_start) = &sub.lag_started_at {
                        if !sub.allow_drop {
                            break 'lag_drop;
                        }

                        let duration = lag_start.elapsed();
                        if duration < self.settings.max_time_lag {
                            break 'lag_drop;
                        }

                        duration
                    } else {
                        tracing::info!(
                            "Subscriber({}): lag started thresh({}) <= lag({})",
                            sub.id,
                            self.settings.lag_start_threshold,
                            lag,
                        );

                        self.metrics
                            .lagging_subs_gauge
                            .fetch_add(1, Ordering::Relaxed);
                        sub.lag_started_at = Some(Instant::now());
                        break 'lag_drop;
                    };

                    /* drop connection due to high lag */

                    tracing::info!(
                        "Subscriber({}): lag too high for too long ({:?}), dropping",
                        sub.id,
                        lag_duration,
                    );

                    assert!(sub.allow_drop);
                    self.metrics
                        .lagging_subs_gauge
                        .fetch_sub(1, Ordering::Relaxed);
                    self.metrics
                        .disconnect_count
                        .fetch_add(1, Ordering::Relaxed);
                    self.subscribers.swap_remove(i);
                    continue 'next_sub;
                }

                if sub.pending.is_some() {
                    blocked_senders_idx.push(i);
                }

                min_sub_sequence = min_sub_sequence.min(sub.next_sequence);
                i += 1;
            }

            self.metrics
                .active_subs_gauge
                .store(self.subscribers.len() as u64, Ordering::Relaxed);
            self.metrics
                .min_sub_sequence_gauge
                .store(min_sub_sequence, Ordering::Relaxed);

            /* trim queue */
            {
                let mut keep_seq =
                    self.next_queue_seq.max(self.settings.min_history) - self.settings.min_history;
                keep_seq = keep_seq.min(min_sub_sequence);

                if oldest_queue_sequence < keep_seq {
                    let remove_count = keep_seq - oldest_queue_sequence;
                    if remove_count != 0 {
                        let _ = self.queue.drain(0..remove_count as usize);
                    }

                    self.metrics
                        .oldest_sequence
                        .store(oldest_queue_sequence + remove_count, Ordering::Relaxed);
                }
            }

            let mut sending_sub_sends = Vec::with_capacity(blocked_senders_idx.len());
            {
                let mut taken = 0;
                let mut remaining = &mut self.subscribers[..];

                for index in &blocked_senders_idx {
                    /* note: messy to make compiler work without unsafe */
                    let sub = {
                        let (old, next) = remaining.split_at_mut(*index - taken + 1);
                        taken += old.len();
                        remaining = next;
                        old.last_mut().unwrap()
                    };

                    let Subscriber {
                        pending: next,
                        next_sequence,
                        tx,
                        ..
                    } = sub;

                    sending_sub_sends.push(PendingSubSend {
                        next,
                        next_sequence,
                        reserve: tx.reserve(),
                    });
                }
            }

            let can_receive_more = !self.rx.is_closed() && self.queue.len() < self.queue.capacity();
            let new_client_closed = self.new_client.is_closed();
            let next_req_closed = self.next_req_rx.is_closed();

            let mut timeout =
                tokio::time::sleep(self.settings.max_time_lag.max(Duration::from_millis(100)));
            let mut canceled = self.shutdown.cancelled();

            poll_fn(|cx| {
                let canceled = unsafe { Pin::new_unchecked(&mut canceled) };
                if let Poll::Ready(()) = canceled.poll(cx) {
                    return Poll::Ready(());
                }

                let mut has_ready = false;

                for pending in &mut sending_sub_sends {
                    let reserve = unsafe { Pin::new_unchecked(&mut pending.reserve) };

                    if let Poll::Ready(result) = reserve.poll(cx) {
                        has_ready = true;

                        if let Ok(reserved) = result {
                            if let Some(next) = pending.next.take() {
                                reserved.send((*pending.next_sequence, next));
                                *pending.next_sequence += 1;
                            }
                        }
                    }
                }

                if !new_client_closed {
                    if let Poll::Ready(item) = self.new_client.poll_recv(cx) {
                        has_ready = true;
                        self.next_client = item;
                    }
                }

                if can_receive_more {
                    if let Poll::Ready(item) = self.rx.poll_recv(cx) {
                        has_ready = true;

                        if let Some((seq, msg)) = item {
                            assert_eq!(seq, self.next_queue_seq);
                            self.queue.push_back((seq, msg));
                            self.next_queue_seq += 1;
                        }
                    }
                }

                if !next_req_closed {
                    if let Poll::Ready(item) = self.next_req_rx.poll_recv(cx) {
                        has_ready = true;
                        self.next_req = item;
                    }
                }

                if !has_ready {
                    has_ready = unsafe { Pin::new_unchecked(&mut timeout) }.poll(cx).is_ready();
                }

                if has_ready {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    pub fn setup_logging() {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    }

    #[tokio::test]
    async fn subscribers_replace_sender_test() {
        setup_logging();

        let (subs, mut tx1) =
            SequencedBroadcast::<&'static str>::new(0, SequencedBroadcastSettings::default());
        tx1.safe_send(0, "Hello WOrld").await.unwrap();
        tx1.safe_send(1, "What the heck").await.unwrap();

        let mut tx2 = tokio::time::timeout(Duration::from_millis(10), subs.replace_sender(2))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            tx1.send("fail").await,
            Err(SequencedSenderError::ChannelClosed("fail"))
        );

        tx2.safe_send(2, "Hehe").await.unwrap();

        let mut sub_1 = subs.add_client(0, true).await.unwrap();
        assert_eq!((0, "Hello WOrld"), sub_1.recv().await.unwrap());
        assert_eq!((1, "What the heck"), sub_1.recv().await.unwrap());
        assert_eq!((2, "Hehe"), sub_1.recv().await.unwrap());

        let mut tx3 = tokio::time::timeout(Duration::from_millis(10), subs.replace_sender(2))
            .await
            .unwrap()
            .unwrap();
        drop(tx1);
        drop(tx2);
        tokio::time::sleep(Duration::from_millis(200)).await;

        tx3.safe_send(3, "It works").await.unwrap();
        assert_eq!((3, "It works"), sub_1.recv().await.unwrap());
    }

    #[tokio::test]
    async fn subscribers_catchup_test() {
        setup_logging();

        let (subs, mut tx) =
            SequencedBroadcast::<&'static str>::new(0, SequencedBroadcastSettings::default());
        tx.send("Hello WOrld").await.unwrap();
        tx.send("What the heck").await.unwrap();

        let mut sub_1 = subs.add_client(0, true).await.unwrap();
        assert_eq!((0, "Hello WOrld"), sub_1.recv().await.unwrap());
        assert_eq!((1, "What the heck"), sub_1.recv().await.unwrap());

        let mut sub_2 = subs.add_client(0, true).await.unwrap();
        assert_eq!((0, "Hello WOrld"), sub_2.recv().await.unwrap());
        assert_eq!((1, "What the heck"), sub_2.recv().await.unwrap());

        let mut sub_3 = subs.add_client(1, true).await.unwrap();
        assert_eq!((1, "What the heck"), sub_3.recv().await.unwrap());

        tx.send("Hehe").await.unwrap();
        assert_eq!((2, "Hehe"), sub_1.recv().await.unwrap());
        assert_eq!((2, "Hehe"), sub_2.recv().await.unwrap());
        assert_eq!((2, "Hehe"), sub_3.recv().await.unwrap());
    }

    #[tokio::test]
    async fn subscribers_test() {
        setup_logging();

        let (subs, mut tx) =
            SequencedBroadcast::<&'static str>::new(10, SequencedBroadcastSettings::default());
        tx.send("Hello WOrld").await.unwrap();
        tx.send("What the heck").await.unwrap();

        let mut sub = subs.add_client(10, true).await.unwrap();
        assert_eq!((10, "Hello WOrld"), sub.recv().await.unwrap());
        assert_eq!((11, "What the heck"), sub.recv().await.unwrap());

        assert!(subs.add_client(10, true).await.is_some());
        assert!(subs.add_client(11, true).await.is_some());
        assert!(subs.add_client(12, true).await.is_some());
        assert!(subs.add_client(13, true).await.is_none());
        assert!(subs.add_client(9, true).await.is_none());

        tx.send("Butts").await.unwrap();
        assert_eq!((12, "Butts"), sub.recv().await.unwrap());

        tokio::time::sleep(Duration::from_millis(1)).await;

        tracing::info!("Metrics: {:?}", subs.metrics_ref());
    }

    #[tokio::test]
    async fn subscribers_dont_drop_test() {
        setup_logging();

        let (subs, mut tx) = SequencedBroadcast::<i64>::new(
            1,
            SequencedBroadcastSettings {
                max_time_lag: Duration::from_millis(100),
                ..Default::default()
            },
        );

        let mut sub = subs.add_client(1, false).await.unwrap();

        loop {
            if tokio::time::timeout(Duration::from_secs(1), tx.send(1))
                .await
                .is_err()
            {
                break;
            }
        }

        assert!(tokio::time::timeout(Duration::from_secs(1), tx.send(1))
            .await
            .is_err());
        assert_eq!((1, 1), sub.recv().await.unwrap());

        assert!(
            tokio::time::timeout(Duration::from_millis(10), tx.send(1000))
                .await
                .is_ok()
        );

        let sub_mut = &mut sub;

        tokio::time::timeout(Duration::from_millis(100), async move {
            loop {
                let (_, num) = sub_mut.recv().await.unwrap();
                if num == 1000 {
                    break;
                }
            }
        })
        .await
        .unwrap();

        assert!(
            tokio::time::timeout(Duration::from_millis(10), tx.send(2000))
                .await
                .is_ok()
        );
        assert_eq!(2000, sub.recv().await.unwrap().1);
    }

    #[tokio::test]
    async fn subscribers_no_clients_test() {
        setup_logging();

        let (subs, mut tx) =
            SequencedBroadcast::<&'static str>::new(1, SequencedBroadcastSettings::default());
        let (subs, mut tx) = tokio::time::timeout(Duration::from_secs(1), async move {
            for _ in 0..1_000_000 {
                tx.send("Hello World").await.unwrap();
            }

            tracing::info!("Sent 1M messages");

            while tx.seq() != subs.metrics_ref().next_sequence.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }

            tracing::info!("All 1M messages have been processed");

            (subs, tx)
        })
        .await
        .unwrap();

        let seq = tx.seq();
        tracing::info!("Seq: {}", seq);

        let mut sub = subs.add_client(seq - 1, true).await.unwrap();
        tx.send("Test").await.unwrap();

        assert_eq!((seq - 1, "Hello World"), sub.recv().await.unwrap());
        assert_eq!((seq, "Test"), sub.recv().await.unwrap());
    }

    #[tokio::test]
    async fn subscribers_drops_slow_sub() {
        setup_logging();

        let (subs, mut tx) = SequencedBroadcast::<i64>::new(
            1,
            SequencedBroadcastSettings {
                max_time_lag: Duration::from_secs(1),
                subscriber_channel_len: 4,
                lag_start_threshold: 64,
                lag_end_threshold: 32,
                ..Default::default()
            },
        );

        let mut fast_client = subs.add_client(1, true).await.unwrap();
        let mut slow_client = subs.add_client(1, true).await.unwrap();

        let send_task = tokio::spawn(async move {
            let mut i = 0;
            /* 5 seconds of sending */

            for _ in 0..1_000 {
                tokio::time::sleep(Duration::from_millis(5)).await;
                i += 1;
                tx.send(i).await.unwrap();
            }

            tracing::info!("Done sending");
            drop(subs);

            i
        });

        let fast_recv_task = tokio::spawn(async move {
            let mut last = None;
            while let Some(recv) = fast_client.recv().await {
                last = Some(recv.1);
            }
            tracing::info!("Fast Done: {:?}", last);
            last.unwrap()
        });

        let slow_recv_task = tokio::spawn(async move {
            let mut last = None;
            while let Some(recv) = slow_client.recv().await {
                last = Some(recv.1);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            tracing::info!("Slow done: {:?}", last);
            last.unwrap()
        });

        let sent_i = send_task.await.unwrap();
        let fast_recv_i = fast_recv_task.await.unwrap();
        let slow_recv_i = slow_recv_task.await.unwrap();

        assert_eq!(sent_i, 1000);
        assert_eq!(fast_recv_i, 1000);
        assert_eq!(slow_recv_i, 19);
    }
}
