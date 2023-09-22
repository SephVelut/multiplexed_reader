#![allow(warnings)]

use std::{sync::Arc, pin::Pin, task::{Context, Poll}};

use async_broadcast::{Receiver, Sender, TrySendError};
use futures_util::{Stream, FutureExt, StreamExt};
use miette::Diagnostic;
use pin_project::pin_project;
use smol::lock::{MutexGuardArc, Mutex};
use thiserror::Error;
use tracing::{error, info, warn};

#[derive(Error, Diagnostic, Debug, Clone)]
pub enum ReaderError<T: std::fmt::Debug> {
    #[error("{}", self)]
    SendError(TrySendError<T>),

    // can increase channel size cap + increment to
    // compensate and rebroadcast msg to the other
    // listeners. Application using shared listener
    // will need to do the rebroadcasting. But application
    // should have a spare Sender to use for that, otherwise
    // channel would be closed anyway.
    #[error("{}", self)]
    ChannelFull { msg: T, cap: usize },
}

#[pin_project]
pub struct LockingReader<R> {
    mutex: Arc<Mutex<R>>,
    guard: Option<MutexGuardArc<R>>,
}

impl<R> Clone for LockingReader<R> {
    fn clone(&self) -> Self {
        LockingReader { 
            mutex: self.mutex.clone(),
            guard: None 
        }
    }
}

impl<R> LockingReader<R> {
    pub fn new(r: R) -> Self {
        LockingReader { 
            mutex: Arc::new(Mutex::new(r)),
            guard: None 
        }
    }
}

impl<R> Stream for LockingReader<R> 
where
    R: Stream + Unpin
{
    type Item = R::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        let guard = if let None = this.guard.as_mut() {
            *this.guard = Some(futures::ready!(this.mutex.lock_arc().poll_unpin(cx)));
            this.guard.as_mut().unwrap()
        } else {
            this.guard.as_mut().unwrap()
        };

        futures::ready!(guard.poll_next_unpin(cx)).into()
    }
}

#[pin_project]
pub struct MultiplexedReader<R, T> {
    receiver: Receiver<T>,
    sender:   Sender<T>,
    reader:   R,
}

impl<R, T> MultiplexedReader<R, T> {
    // we create a new receiver using clone() which will place the new 
    // receiver in queue for messages sent before and after it is called.
    pub fn new_receive_before(reader: R, sender: &Sender<T>, receiver: &Receiver<T>) -> Self {
        let sender  = sender.clone();
        assert!(
            !sender.is_closed(), 
            "not required but broadcast channel silently remains closed 
            once closed which could lead to heisenbugs"
        );
        assert!(!sender.overflow());
        assert!(sender.await_active());

        // any messages already in the channel will be receivable
        let receiver = receiver.clone();

        MultiplexedReader { 
            receiver, 
            sender, 
            reader,
        }
    }

    // we create a new receiver using new_receiver() which will place the
    // new receiver in queue for messages sent after it is called, not before.
    pub fn new(reader: R, sender: &Sender<T>) -> Self {
        // todo: we technically should not be cloning a sender as a broadcast
        // channel is not the proper mechanism for the shared listener.
        // ought to use a spmc or refactor to aquire an Arc Mutex Sender
        let sender  = sender.clone();
        assert!(
            !sender.is_closed(), 
            "not required but broadcast channel silently remains closed 
            once closed which could lead to heisenbugs"
        );
        assert!(!sender.overflow());
        assert!(sender.await_active());

        // any messages already in the channel will not be receivable
        let receiver = sender.new_receiver();

        MultiplexedReader { 
            receiver, 
            sender, 
            reader,
        }
    }
}

impl<R> Stream for MultiplexedReader<R, <R as Stream>::Item> 
where
    R: Stream + Unpin,
    <R as Stream>::Item: Clone + std::fmt::Debug,
{
    type Item = Result<<R as Stream>::Item, ReaderError<<R as Stream>::Item>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        let mut reader_done = false;
        match this.reader.poll_next_unpin(cx) {
            Poll::Ready(Some(msg)) => {
                assert!(!this.sender.overflow());
                match this.sender.try_broadcast(msg) {
                    Ok(None) => (),
                    Err(TrySendError::Full(msg)) => {
                        error!("full channel encountered | msg: {:?}", msg);
                        return Poll::Ready(Some(Err(ReaderError::ChannelFull { msg, cap: this.sender.capacity() })))
                    },
                    Err(TrySendError::Inactive(msg)) => {
                        info!("inactive channel encountered | msg: {:?}", msg);
                        return Poll::Ready(Some(Ok(msg)))
                    },
                    Err(TrySendError::Closed(msg)) => {
                        warn!("closed channel encountered | msg: {:?}", msg);
                        return Poll::Ready(Some(Ok(msg)))
                    },
                    // overflowed
                    Ok(Some(msg)) => unreachable!("overflowed channel encountered | msg: {:?}", msg),
                };
            },
            Poll::Ready(None) => reader_done = true,
            Poll::Pending => (),
        };

        match this.receiver.poll_next_unpin(cx) {
            Poll::Ready(Some(msg)) => return Poll::Ready(Some(Ok(msg))),
            Poll::Ready(None)      => assert!(this.receiver.is_closed()),
            Poll::Pending          => (),
        }

        if reader_done {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{pin::Pin, task::{Context, Poll}, time::Duration, sync::Arc};

    use futures_util::{Stream, StreamExt};
    use itertools::Itertools;
    use mockall::mock;
    use rand::{thread_rng, Rng, SeedableRng};
    use rstest::{fixture, rstest};
    use smol::lock::Mutex;

    use crate::{MultiplexedReader, ReaderError, LockingReader};

    #[rstest]
    #[timeout(Duration::from_millis(1000))]
    #[smol_potat::test]
    async fn broadcast_to_each_other() {
        let num_of_messages = 1000;
        let num_of_listeners = 100;
        if std::env::var("SMOL_THREADS").is_err() {
            std::env::set_var("SMOL_THREADS", "16");
        }

        let messages = messages(num_of_messages).collect_vec();

        let reader       = LockingReader::new(finite_reader(messages.clone().into_iter()));
        let (mut tx, rx) = async_broadcast::broadcast::<ReaderMessage>(num_of_messages * 2);
        let rx = rx.deactivate();

        let mut listeners = Vec::with_capacity(num_of_listeners);
        for _ in 0..num_of_listeners {
            listeners.push(MultiplexedReader::new(reader.clone(), &tx));
        }

        let mut collector = Arc::new(Mutex::new(Vec::with_capacity(num_of_messages * num_of_listeners)));
        let mut tasks     = Vec::with_capacity(num_of_listeners);
        for mut listener in listeners.into_iter() {
            let mut collector = Arc::clone(&collector);
            let task = smol::spawn(async move {
                let mut sub_collector = Vec::with_capacity(num_of_messages);
                let mut rng = rand::rngs::StdRng::from_entropy();
                for x in 0..num_of_messages {
                    if x % 2 == 0 {
                        smol::Timer::after(std::time::Duration::from_micros(1)).await;
                    }

                    if rng.gen_bool(0.1) {
                        smol::Timer::after(std::time::Duration::from_millis(1)).await;
                    }

                    sub_collector.push(listener.next().await);
                }

                collector.lock().await.append(&mut sub_collector);
            });

            tasks.push(task);
        }

        // wait for all listener tasks to finish
        for task in tasks.into_iter() {
            task.await;
        }

        let mut collector = Arc::into_inner(collector).unwrap().into_inner();
        let collected = collector
            .into_iter()
            .map(|msg| msg.unwrap().unwrap())
            .counts();
        let against = messages.into_iter()
            .flat_map(|item| std::iter::repeat(item).take(num_of_listeners))
            .counts();
        pretty_assertions::assert_eq!(collected, against);
    }

    #[rstest]
    #[timeout(Duration::from_millis(1))]
    #[smol_potat::test]
    async fn exhausts_reader_before_channel() {
        let receiver_message  = message(Some("aaaaaaaa"), 10);
        let reader_messages   = messages(10).collect_vec();

        let (mut tx, rx) = async_broadcast::broadcast::<ReaderMessage>(21);
        let rx = rx.deactivate();

        let reader       = LockingReader::new(finite_reader(reader_messages.clone().into_iter()));
        let mut listener = MultiplexedReader::new(reader, &tx);

        for _ in 0..20 { tx.broadcast(receiver_message.clone()).await; }

        let mut results = Vec::with_capacity(30);
        for _ in 0..30 {
            results.push(listener.next().await.unwrap().unwrap());
        };

        let events = results.into_iter().counts();
        let against = reader_messages.into_iter()
            .chain(std::iter::repeat(receiver_message).take(20))
            .counts();

        pretty_assertions::assert_eq!(events, against);
    }

    #[rstest]
    #[timeout(Duration::from_millis(1))]
    #[smol_potat::test]
    async fn ends_when_reader_ends(message: String) {
        let mut reader = MockStreamingBytes::new();
        let m1 = message.clone();
        reader.expect_poll_next().once().return_once(move |_| Poll::Ready(Some(m1)));
        let m2 = message.clone();
        reader.expect_poll_next().once().return_once(move |_| Poll::Ready(Some(m2)));
        reader.expect_poll_next().once().return_once(move |_| Poll::Ready(None));

        let (mut tx, mut rx) = async_broadcast::broadcast::<ReaderMessage>(2);
        let rx = rx.deactivate();

        let reader       = LockingReader::new(reader);
        let mut listener = MultiplexedReader::new(reader, &tx);

        listener.next().await;
        listener.next().await;
        assert!(matches!(listener.next().await, None));
    }

    #[rstest]
    #[timeout(Duration::from_millis(1))]
    #[smol_potat::test]
    async fn reader_only_with_active_receivers(messages: impl Iterator<Item = String> + Send + 'static) {
        let reader = bounded_reader(1, messages);
        let (mut tx, mut rx) = async_broadcast::broadcast::<ReaderMessage>(1);

        let reader       = LockingReader::new(reader);
        let mut listener = MultiplexedReader::new(reader, &tx);

        let message = listener.next().await.unwrap().unwrap();
        pretty_assertions::assert_eq!(message, message);
    }

    #[rstest]
    #[timeout(Duration::from_millis(1))]
    #[smol_potat::test]
    async fn receiver_only(message: String) {
        let reader = none_reader();
        let (mut tx, mut rx) = async_broadcast::broadcast::<ReaderMessage>(1);

        let reader       = LockingReader::new(reader);
        let mut listener = MultiplexedReader::new(reader, &tx);

        assert!(matches!(tx.broadcast(message).await, Ok(None)));

        let message = listener.next().await.unwrap().unwrap();
        pretty_assertions::assert_eq!(message, message);
    }

    #[rstest]
    #[timeout(Duration::from_millis(1))]
    #[smol_potat::test]
    async fn can_be_constructed_to_receive_previous_broadcasts(message: String) {
        let reader = none_reader();
        let (mut tx, mut rx) = async_broadcast::broadcast::<ReaderMessage>(1);
        assert!(matches!(tx.broadcast(message).await, Ok(None)));

        let reader       = LockingReader::new(reader);
        let mut listener = MultiplexedReader::new_receive_before(reader, &tx, &rx);

        let message = listener.next().await.unwrap().unwrap();
        pretty_assertions::assert_eq!(message, message);
    }

    #[rstest]
    #[timeout(Duration::from_millis(1))]
    #[smol_potat::test]
    async fn returns_error_sending_to_filled_up_channel(message: String) {
        let mut reader = MockStreamingBytes::new();
        let m1 = message.clone();
        let m2 = message.clone();
        reader.expect_poll_next().once().return_once(move |_| Poll::Ready(Some(m1)));
        reader.expect_poll_next().once().return_once(move |_| Poll::Ready(Some(m2)));

        let (mut tx, mut rx) = async_broadcast::broadcast::<ReaderMessage>(1);

        let reader       = LockingReader::new(reader);
        let mut listener = MultiplexedReader::new(reader, &tx);

        listener.next().await;
        let ReaderError::ChannelFull { msg, cap } = listener.next().await.unwrap().unwrap_err() else{unreachable!()};
        pretty_assertions::assert_eq!(message, msg);
        pretty_assertions::assert_eq!(cap, 1);
    }

    #[rstest]
    #[timeout(Duration::from_millis(1))]
    #[smol_potat::test]
    async fn returns_error_sending_to_already_full_channel(messages: impl Iterator<Item = String> + Send + 'static) {
        let messages = messages.take(10).collect_vec();
        let reader = bounded_reader(1, messages.clone().into_iter());
        let (mut tx, mut rx) = async_broadcast::broadcast::<ReaderMessage>(1);
        assert!(matches!(tx.broadcast(messages.first().unwrap().clone()).await, Ok(None)));

        let reader       = LockingReader::new(reader);
        let mut listener = MultiplexedReader::new(reader, &tx);

        let ReaderError::ChannelFull { msg, cap } = listener.next().await.unwrap().unwrap_err() else{unreachable!()};
        pretty_assertions::assert_eq!(messages.first().unwrap().clone(), msg);
        pretty_assertions::assert_eq!(cap, 1);
    }

    #[rstest]
    #[timeout(Duration::from_millis(1))]
    #[smol_potat::test]
    async fn ignores_send_when_no_active_receivers(messages: impl Iterator<Item = String> + Send + 'static) {
        let reader = bounded_reader(1, messages);
        let (mut tx, mut rx) = async_broadcast::broadcast::<ReaderMessage>(1);
        let rx = rx.deactivate();

        let reader       = LockingReader::new(reader);
        let mut listener = MultiplexedReader::new(reader, &tx);

        let message = listener.next().await.unwrap().unwrap();
        pretty_assertions::assert_eq!(message, message);
        assert!(tx.is_empty());
    }

    type ReaderMessage = String;

    mock!{
        #[derive(Debug)]
        StreamingBytes {}
        impl Stream for StreamingBytes {
            type Item = ReaderMessage;
            fn poll_next<'a>(
                self: Pin<&mut Self>,
                cx: &mut Context<'a>,
            ) -> Poll<Option<<MockStreamingBytes as Stream>::Item>>;
        }
    }

    #[fixture]
    fn none_reader() -> impl Stream<Item = ReaderMessage> {
        let mut reader = MockStreamingBytes::new();
        reader.expect_poll_next().returning(|_| {
            Poll::Ready(None)
        });

        reader
    }

    #[fixture]
    fn never_reader() -> impl Stream<Item = ReaderMessage> {
        let mut reader = MockStreamingBytes::new();
        reader.expect_poll_next().never();

        reader
    }

    // used when you dont expect reader to be called more than n times. calling more than n times
    // should panic.
    #[fixture]
    fn bounded_reader(#[default(0)] n: usize, mut messages: impl Iterator<Item = String> + Send + 'static) -> impl Stream<Item = String> {
        assert!(n > 0, "n must be > 0, use never_reader");
        let mut reader = MockStreamingBytes::new();
        let mut messages = messages.take(n);
        if n == 1 {
            reader 
                .expect_poll_next()
                .once()
                .return_once(move |_| {
                    Poll::Ready(Some(messages.next().unwrap()))
                });
        } else {
            reader 
                .expect_poll_next()
                .times(n)
                .returning(move |_| {
                    Poll::Ready(Some(messages.next().unwrap()))
                });
        }

        reader
    }

    #[fixture]
    fn infinite_reader(mut messages: impl Iterator<Item = String> + Send + 'static) -> impl Stream<Item = String> {
        let mut reader = MockStreamingBytes::new();

        reader 
            .expect_poll_next()
            .times(..)
            .returning(move |_| {
                if let message @ Some(_) = messages.next() {
                    Poll::Ready(message)
                } else {
                    Poll::Pending
                }
            });

        reader
    }

    /// use when reader should signal None for end of stream. messages iterator must be bound to a
    /// limit otherwise this reader will output infinitely.
    #[fixture]
    fn finite_reader(mut messages: impl Iterator<Item = String> + Send + 'static) -> impl Stream<Item = String> {
        let mut reader = MockStreamingBytes::new();

        reader 
            .expect_poll_next()
            .times(..)
            .returning(move |_| {
                if let message @ Some(_) = messages.next() {
                    Poll::Ready(message)
                } else {
                    Poll::Ready(None)
                }
            });

        reader
    }

    #[fixture]
    fn messages(#[default(usize::MAX)] take: usize) -> impl Iterator<Item = String> + Send + 'static + Clone {
        std::iter::from_fn(move || {
            Some(message(None, take))
        }).take(take)
    }

    #[fixture]
    fn message(#[default(None)] body: Option<&str>, #[default(100)] max: usize) -> String {
        if let Some(body) = body {
            body.to_string()
        } else {
            let n: usize = thread_rng().gen_range(1..=max);
            format!("{n}")
        }
    }
}
