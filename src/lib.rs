use std::{
    collections::VecDeque, fmt::Debug, future::{poll_fn, Future}, sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    }, task::Poll, time::{Duration, Instant}
};

use tokio::sync::{
    mpsc::{channel, error::{SendError, TryRecvError, TrySendError}, Permit, Receiver, Sender},
    oneshot,
};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

pub struct SequencedBroadcast<T> {
    new_client_tx: Sender<NewClient<T>>,
    metrics: Arc<SequencedBroadcastMetrics>,
    shutdown: CancellationToken,
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
    rx_closed: bool,
    rx_full: bool,
    next_rx: Option<(u64, T)>,
    next_sub_id: u64,
    new_client_rx: Receiver<NewClient<T>>,
    next_client: Option<NewClient<T>>,
    subscribers: Vec<Subscriber<T>>,
    queue: VecDeque<(u64, T)>,
    next_queue_seq: u64,
    metrics: Arc<SequencedBroadcastMetrics>,
    settings: SequencedBroadcastSettings,
    shutdown: CancellationToken,
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
        let (tx, rx) = channel(1024);
        let tx = SequencedSender::new(next_seq, tx);
        let rx = SequencedReceiver::new(next_seq, rx);

        (
            Self::new2(rx, settings),
            tx
        )
    }

    pub fn new2(receiver: SequencedReceiver<T>, settings: SequencedBroadcastSettings) -> Self {
        let queue_cap = ((settings.lag_start_threshold as usize)
            .next_power_of_two()
            .max(1024)
            * 2)
        .max((settings.min_history as usize).next_power_of_two());

        assert!(settings.lag_end_threshold <= settings.lag_start_threshold);

        let (client_tx, client_rx) = channel(32);

        let metrics = Arc::new(SequencedBroadcastMetrics {
            oldest_sequence: AtomicU64::new(receiver.next_seq),
            next_sequence: AtomicU64::new(receiver.next_seq),
            ..Default::default()
        });

        let shutdown = CancellationToken::new();
        let current_span = tracing::Span::current();

        tokio::spawn(
            Worker {
                rx: receiver.receiver,
                next_rx: None,
                rx_full: false,
                rx_closed: false,
                next_sub_id: 1,
                new_client_rx: client_rx,
                next_client: None,
                subscribers: Vec::with_capacity(32),
                queue: VecDeque::with_capacity(queue_cap),
                next_queue_seq: receiver.next_seq,
                metrics: metrics.clone(),
                settings,
                shutdown: shutdown.clone(),
            }
            .start()
            .instrument(current_span),
        );

        Self {
            new_client_tx: client_tx,
            metrics,
            shutdown,
        }
    }

    pub async fn add_client(
        &self,
        next_sequence: u64,
        allow_drop: bool,
    ) -> Result<SequencedReceiver<T>, NewClientError> {
        let (tx, rx) = oneshot::channel();

        self.new_client_tx
            .send(NewClient {
                response: tx,
                allow_drop,
                next_sequence,
            })
            .await
            .expect("Failed to queue new subscriber, worker crashed");

        rx.await.expect("worker closed")
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
    response: oneshot::Sender<Result<SequencedReceiver<T>, NewClientError>>,
    next_sequence: u64,
    allow_drop: bool,
}

#[derive(Debug)]
pub enum NewClientError {
    SequenceTooFarAhead { seq: u64, max: u64 },
    SequenceTooFarBehind { seq: u64, min: u64 },
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

impl<T: Send + Clone + 'static> Worker<T> {
    async fn start(mut self) {
        loop {
            tokio::task::yield_now().await;

            if self.next_client.is_none() {
                self.next_client = self.new_client_rx.try_recv().ok();
            }

            if self.shutdown.is_cancelled() {
                tracing::info!("Stopping worker due to shutdown");
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
                    self.next_client = self.new_client_rx.try_recv().ok();
                }

                while let Some(new) = self.next_client.take() {
                    self.next_client = self.new_client_rx.try_recv().ok();

                    /* Sequence in valid range */
                    if new.next_sequence < min_allowed_seq
                        || self.next_queue_seq < new.next_sequence
                    {
                        self.metrics
                            .new_client_drop_count
                            .fetch_add(1, Ordering::Relaxed);

                        if new.next_sequence < min_allowed_seq {
                            tracing::info!(
                                "Subscriber rejected, seq({}) < min_allowed({})",
                                new.next_sequence,
                                min_allowed_seq
                            );

                            let _ = new.response.send(Err(NewClientError::SequenceTooFarBehind {
                                seq: new.next_sequence,
                                min: min_allowed_seq
                            }));
                        } else {
                            tracing::info!(
                                "Subscriber rejected, max_seq({}) < seq({})",
                                self.next_queue_seq,
                                new.next_sequence
                            );

                            let _ = new.response.send(Err(NewClientError::SequenceTooFarAhead {
                                seq: new.next_sequence,
                                max: self.next_queue_seq
                            }));
                        }

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

                    if new.response.send(Ok(rx)).is_ok() {
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
                }
            }

            /* fill queue with available data from rx */
            'fill_rx: {
                if self.next_rx.is_none() {
                    self.next_rx = self.rx.try_recv().ok();
                }

                let mut remaining_msg_count = self.rx_space().min(1024);
                if remaining_msg_count == 0 {
                    if !self.rx_full {
                        self.rx_full = true;
                        assert_eq!(self.queue.len(), self.queue.capacity());
                        tracing::info!("Reached queue capacity {}", self.queue.len());
                    }

                    break 'fill_rx;
                }

                self.rx_full = false;

                while let Some((seq, item)) = self.next_rx.take() {
                    self.next_rx = match self.rx.try_recv() {
                        Ok(v) => Some(v),
                        Err(TryRecvError::Disconnected) => {
                            self.rx_closed = true;
                            None
                        }
                        Err(TryRecvError::Empty) => None,
                    };

                    assert_eq!(seq, self.next_queue_seq, "sequence is invalid");
                    self.queue.push_back((seq, item));
                    self.next_queue_seq += 1;

                    remaining_msg_count -= 1;
                    if remaining_msg_count == 0 {
                        break;
                    }
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

            let max_seq = oldest_queue_sequence + self.queue.len() as u64;
            let lag_start_seq = max_seq.max(self.settings.lag_start_threshold) - self.settings.lag_start_threshold;
            let lag_end_seq = lag_start_seq.max(max_seq.max(self.settings.lag_end_threshold) - self.settings.lag_end_threshold);

            let mut min_sub_sequence_calc = self.next_queue_seq;
            let mut earliest_lag_start_at_calc: Option<Instant> = None;

            let mut i = 0;
            'next_sub: while i < self.subscribers.len() {
                let sub = &mut self.subscribers[i];

                /* make sure sub is still valid */
                if (sub.allow_drop && sub.next_sequence < oldest_queue_sequence) || sub.tx.is_closed() {
                    if sub.tx.is_closed() {
                        tracing::info!("Subscriber({}): channel closed, dropping", sub.id);
                    } else {
                        tracing::warn!(
                            "Subscriber({}): lag behind available data ({} < {}), dropping",
                            sub.id,
                            sub.next_sequence,
                            oldest_queue_sequence
                        );
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

                /* write_to_sub */
                let mut offset = {
                    assert!(sub.next_sequence >= oldest_queue_sequence);
                    let offset = (sub.next_sequence - oldest_queue_sequence) as usize;
                    assert!(sub.next_sequence <= self.next_queue_seq, "sub cannot be ahead of queue sequence");
                    assert!(offset <= self.queue.len(), "sub cannot be ahead of queue sequence");
                    offset
                };

                /* prep next message to send */
                if sub.pending.is_none() {
                    /* fully caught up */
                    if self.queue.len() == offset {
                        i += 1;
                        continue 'next_sub;
                    }

                    /* make next item pending */
                    let (seq, item) = self.queue.get(offset).unwrap();
                    assert_eq!(*seq, sub.next_sequence);
                    sub.pending = Some(item.clone());
                }

                /* send as much as possible */
                while let Some(next) = sub.pending.take() {
                    match sub.tx.try_send((sub.next_sequence, next)) {
                        Ok(_) => {
                            sub.next_sequence += 1;
                            offset += 1;

                            if self.queue.len() == offset {
                                break;
                            }

                            let (seq, item) = self.queue.get(offset).unwrap();
                            assert_eq!(*seq, sub.next_sequence);
                            sub.pending = Some(item.clone());
                        }
                        Err(TrySendError::Closed(_)) => break,
                        Err(TrySendError::Full((_seq, item))) => {
                            sub.pending = Some(item);
                            break;
                        }
                    }
                }

                if sub.allow_drop {
                    if lag_end_seq <= sub.next_sequence {
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
                    }
                    else if sub.next_sequence < lag_start_seq {
                        if let Some(lag_start) = &sub.lag_started_at {
                            let lag_duration = lag_start.elapsed();

                            if self.settings.max_time_lag < lag_duration {
                                tracing::info!(
                                    "Subscriber({}): lag too high for too long ({:?}), dropping",
                                    sub.id,
                                    lag_duration,
                                );

                                self.metrics
                                    .lagging_subs_gauge
                                    .fetch_sub(1, Ordering::Relaxed);

                                self.metrics
                                    .disconnect_count
                                    .fetch_add(1, Ordering::Relaxed);

                                self.subscribers.swap_remove(i);
                                continue 'next_sub;
                            }
                        } else {
                            sub.lag_started_at = Some(Instant::now());

                            tracing::info!(
                                "Subscriber({}): lag started thresh({}) < lag({})",
                                sub.id,
                                self.settings.lag_start_threshold,
                                max_seq - sub.next_sequence,
                            );

                            self.metrics
                                .lagging_subs_gauge
                                .fetch_add(1, Ordering::Relaxed);
                            }
                    }
                }

                if let Some(lag_started_at) = &sub.lag_started_at {
                    earliest_lag_start_at_calc = match earliest_lag_start_at_calc {
                        Some(v) if v.lt(lag_started_at) => Some(v),
                        _ => sub.lag_started_at
                    };
                }

                min_sub_sequence_calc = min_sub_sequence_calc.min(sub.next_sequence);
                i += 1;
            }

            let min_sub_sequence = min_sub_sequence_calc;

            self.metrics
                .active_subs_gauge
                .store(self.subscribers.len() as u64, Ordering::Relaxed);

            self.metrics
                .min_sub_sequence_gauge
                .store(min_sub_sequence, Ordering::Relaxed);

            /* trim rx queue */
            {
                let keep_seq = min_sub_sequence.min(max_seq.max(self.settings.min_history) - self.settings.min_history);

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

            if self.rx_closed && min_sub_sequence == max_seq {
                tracing::info!("RX closed and all subscribers caught up, closing");
                return;
            }

            let rx_blocked = self.next_rx.is_none() && !self.rx_closed;
            let next_timeout = earliest_lag_start_at_calc.map(|early| {
                let now = Instant::now();
                let expire = early + self.settings.max_time_lag;
                (expire.max(now) - now).max(Duration::from_millis(100))
            });

            /* see if there's more work available without waiting */
            {
                /* update RX */
                if !rx_blocked && 0 < self.rx_space() {
                    tracing::trace!("have more rx");
                    continue;
                }

                /* new client available */
                if self.next_client.is_some() {
                    tracing::trace!("have next client");
                    continue;
                }
            }

            let mut timeout_fut = next_timeout.map(|duration| tokio::time::sleep(duration));
            let mut pending_tx = Vec::new();
            let new_client_rx = &mut self.new_client_rx;
            let new_msg_rx = &mut self.rx;
            let next_rx = &mut self.next_rx;
            let next_client = &mut self.next_client;

            for sub in &mut self.subscribers {
                if sub.pending.is_some() {
                    pending_tx.push((sub.tx.reserve(), &mut sub.pending, &mut sub.next_sequence));
                }
            }

            poll_fn(|cx| {
                if let Some(timeout) = &mut timeout_fut {
                    if unsafe { std::pin::Pin::new_unchecked(timeout) }.poll(cx).is_ready() {
                        tracing::trace!("poll: max lag timer reached");
                        return Poll::Ready(());
                    }
                }

                if rx_blocked {
                    if let Poll::Ready(item) = unsafe { std::pin::Pin::new_unchecked(&mut *new_msg_rx) }.poll_recv(cx) {
                        assert!(next_rx.is_none());

                        *next_rx = item;
                        if next_rx.is_some() {
                            tracing::trace!("poll: new RX available");
                        } else {
                            tracing::trace!("poll: RX closed");
                        }

                        return Poll::Ready(());
                    }
                }

                if let Poll::Ready(item) = unsafe { std::pin::Pin::new_unchecked(&mut *new_client_rx) }.poll_recv(cx) {
                    tracing::trace!("poll: new client");

                    assert!(next_client.is_none());
                    *next_client = item;
                    return Poll::Ready(());
                }

                let mut sent = false;
                for (reserve, pending, next_sequence) in &mut pending_tx {
                    let reserve = unsafe { std::pin::Pin::new_unchecked(reserve) };

                    match reserve.poll(cx) {
                        Poll::Ready(Ok(slot)) => {
                            let item = pending.take().expect("pending missing");
                            let seq = **next_sequence;
                            slot.send((seq, item));
                            **next_sequence = seq + 1;
                            
                            sent = true;
                        }
                        Poll::Ready(Err(_)) => {
                            sent = true;
                        }
                        Poll::Pending => {}
                    }
                }

                if sent {
                    tracing::trace!("poll: subscriber message sent");
                    return Poll::Ready(());
                }

                Poll::Pending
            }).await;


            // let can_receive_more = !self.rx.is_closed() && self.queue.len() < self.queue.capacity();
            // let new_client_closed = self.new_client_rx.is_closed();
            // let mut timeout =
            //     tokio::time::sleep(self.settings.max_time_lag.max(Duration::from_millis(100)));
            // let mut canceled = self.shutdown.cancelled();

            // poll_fn(|cx| {
            //     let canceled = unsafe { Pin::new_unchecked(&mut canceled) };
            //     if let Poll::Ready(()) = canceled.poll(cx) {
            //         return Poll::Ready(());
            //     }

            //     let mut has_ready = false;

            //     for pending in &mut sending_sub_sends {
            //         let reserve = unsafe { Pin::new_unchecked(&mut pending.reserve) };

            //         if let Poll::Ready(result) = reserve.poll(cx) {
            //             has_ready = true;

            //             if let Ok(reserved) = result {
            //                 if let Some(next) = pending.next.take() {
            //                     reserved.send((*pending.next_sequence, next));
            //                     *pending.next_sequence += 1;
            //                 }
            //             }
            //         }
            //     }

            //     if !new_client_closed {
            //         if let Poll::Ready(item) = self.new_client_rx.poll_recv(cx) {
            //             has_ready = true;
            //             self.next_client = item;
            //         }
            //     }

            //     if can_receive_more {
            //         if let Poll::Ready(item) = self.rx.poll_recv(cx) {
            //             has_ready = true;

            //             if let Some((seq, msg)) = item {
            //                 assert_eq!(seq, self.next_queue_seq);
            //                 self.queue.push_back((seq, msg));
            //                 self.next_queue_seq += 1;
            //             }
            //         }
            //     }

            //     if !has_ready {
            //         has_ready = unsafe { Pin::new_unchecked(&mut timeout) }.poll(cx).is_ready();
            //     }

            //     if has_ready {
            //         Poll::Ready(())
            //     } else {
            //         Poll::Pending
            //     }
            // })
            // .await;
        }
    }

    fn rx_space(&self) -> usize {
        self.queue.capacity() - self.queue.len()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    pub fn setup_logging() {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();
        // let _ = tracing_subscriber::fmt().try_init();
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
    async fn sequenced_broadcast_simple_test() {
        setup_logging();

        let (subs, mut tx) =
            SequencedBroadcast::<u64>::new(10, SequencedBroadcastSettings::default());

        let mut client = subs.add_client(10, true).await.unwrap();
        tracing::info!("client added");

        let read_task = tokio::spawn(async move {
            let mut i = 0;
            let mut seq = 10;

            while let Some(msg) = client.recv().await {
                assert_eq!(msg, (seq, i));
                i += 1;
                seq += 1;
            }

            i
        });

        let count = 1024 * 16;

        for i in 0..count {
            tx.send(i).await.unwrap();
        }
        drop(tx);

        let total = read_task.await.unwrap();
        assert_eq!(total, count);
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

        assert!(subs.add_client(10, true).await.is_ok());
        assert!(subs.add_client(11, true).await.is_ok());
        assert!(subs.add_client(12, true).await.is_ok());
        assert!(subs.add_client(13, true).await.is_err());
        assert!(subs.add_client(9, true).await.is_err());

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

        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if tokio::time::timeout(Duration::from_secs(1), tx.send(1))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }).await.expect("client must have been dropped as can still send tx");

        tracing::info!("tx filled");

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
