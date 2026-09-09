use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Wake, Waker};
use std::time::Duration;

use testresult::TestResult;

use super::*;

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn wake_counters<const N: usize>() -> ([Arc<WakeCount>; N], [Waker; N]) {
    let counters = std::array::from_fn(|_| Arc::new(WakeCount::default()));
    let wakers = counters.each_ref().map(|count| Waker::from(count.clone()));
    (counters, wakers)
}

fn test_transmit(destination: SocketAddr) -> Transmit<'static> {
    Transmit {
        destination,
        ecn: None,
        contents: b"independent senders",
        segment_size: None,
        src_ip: None,
    }
}

fn assert_send_ready(result: Poll<io::Result<()>>) {
    assert!(matches!(result, Poll::Ready(Ok(()))), "{result:?}");
}

#[tokio::test]
async fn senders_wake_independently_on_readiness() -> TestResult {
    for order in [[0, 1], [1, 0]] {
        for _ in 0..8 {
            let sink = std::net::UdpSocket::bind("127.0.0.1:0")?;
            let socket = Arc::new(UdpSocket::bind_local_v4(0)?);
            let sender = socket.clone().create_sender();
            let mut senders = [sender.clone(), sender];
            let (counts, wakers) = wake_counters::<2>();
            let transmit = test_transmit(sink.local_addr()?);
            // The current-thread reactor has not run since binding. Both
            // actual senders must register before the writable event.
            for i in order {
                assert!(
                    Pin::new(&mut senders[i])
                        .poll_send(&transmit, &mut Context::from_waker(&wakers[i]))
                        .is_pending()
                );
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
            for i in order {
                assert!(
                    counts[i].0.load(Ordering::SeqCst) > 0,
                    "sender {i} was not woken"
                );
                assert_send_ready(
                    Pin::new(&mut senders[i])
                        .poll_send(&transmit, &mut Context::from_waker(&wakers[i])),
                );
            }
            socket.close().await;
        }
    }
    Ok(())
}

#[tokio::test]
async fn senders_wake_independently_after_rebind() -> TestResult {
    let sink = std::net::UdpSocket::bind("127.0.0.1:0")?;
    let socket = Arc::new(UdpSocket::bind_local_v4(0)?);
    let address = socket.local_addr()?;
    let mut senders = [
        socket.clone().create_sender(),
        socket.clone().create_sender(),
    ];
    let (counts, wakers) = wake_counters::<2>();
    let transmit = test_transmit(sink.local_addr()?);
    for i in 0..2 {
        assert!(
            Pin::new(&mut senders[i])
                .poll_send(&transmit, &mut Context::from_waker(&wakers[i]))
                .is_pending()
        );
    }
    // No reactor yield: these wakes must come from rebind, not readiness
    // on the retired socket. Rebinding also proves no waiter retains it.
    socket.rebind()?;
    assert_eq!(socket.local_addr()?, address);
    for count in &counts {
        assert!(count.0.load(Ordering::SeqCst) > 0);
    }
    tokio::time::sleep(Duration::from_millis(2)).await;
    for i in 0..2 {
        assert_send_ready(
            Pin::new(&mut senders[i]).poll_send(&transmit, &mut Context::from_waker(&wakers[i])),
        );
    }
    socket.close().await;
    Ok(())
}

#[tokio::test]
async fn senders_unregister_on_cancel_and_complete() -> TestResult {
    let sink = std::net::UdpSocket::bind("127.0.0.1:0")?;
    let socket = Arc::new(UdpSocket::bind_local_v4(0)?);
    let mut first = socket.clone().create_sender();
    let mut second = first.clone();
    let (counts, wakers) = wake_counters::<2>();
    let transmit = test_transmit(sink.local_addr()?);
    let guard = socket.socket.write().unwrap();
    assert!(
        Pin::new(&mut first)
            .poll_send(&transmit, &mut Context::from_waker(&wakers[0]))
            .is_pending()
    );
    assert!(
        Pin::new(&mut second)
            .poll_send(&transmit, &mut Context::from_waker(&wakers[1]))
            .is_pending()
    );
    assert!(
        Arc::strong_count(&counts[0]) > 2,
        "first waiter must be independently registered"
    );
    drop(first);
    assert_eq!(
        Arc::strong_count(&counts[0]),
        2,
        "cancelled waiter retained its task"
    );
    drop(guard);
    socket.wake_all();
    assert_eq!(counts[0].0.load(Ordering::SeqCst), 0);
    assert!(counts[1].0.load(Ordering::SeqCst) > 0);
    tokio::time::sleep(Duration::from_millis(2)).await;
    assert_send_ready(
        Pin::new(&mut second).poll_send(&transmit, &mut Context::from_waker(&wakers[1])),
    );
    assert_eq!(
        Arc::strong_count(&counts[1]),
        2,
        "completed waiter retained its task"
    );
    socket.close().await;
    Ok(())
}

#[tokio::test]
async fn senders_reusable_after_ready_and_would_block() -> TestResult {
    let sink = std::net::UdpSocket::bind("127.0.0.1:0")?;
    let socket = Arc::new(UdpSocket::bind_local_v4(0)?);
    let mut senders = [
        socket.clone().create_sender(),
        socket.clone().create_sender(),
    ];
    let (counts, wakers) = wake_counters::<2>();
    let transmit = test_transmit(sink.local_addr()?);
    tokio::time::sleep(Duration::from_millis(2)).await;
    for round in 0..32 {
        let order = if round % 2 == 0 { [0, 1] } else { [1, 0] };
        for i in order {
            assert_send_ready(
                Pin::new(&mut senders[i])
                    .poll_send(&transmit, &mut Context::from_waker(&wakers[i])),
            );
            counts[i].0.store(0, Ordering::SeqCst);
        }
        let guard = socket.socket.write().unwrap();
        for i in order {
            assert_eq!(
                senders[i].try_send(&transmit).unwrap_err().kind(),
                io::ErrorKind::WouldBlock
            );
            assert!(
                Pin::new(&mut senders[i])
                    .poll_send(&transmit, &mut Context::from_waker(&wakers[i]))
                    .is_pending()
            );
        }
        drop(guard);
        socket.wake_all();
        for i in order {
            assert!(counts[i].0.load(Ordering::SeqCst) > 0);
            assert_send_ready(
                Pin::new(&mut senders[i])
                    .poll_send(&transmit, &mut Context::from_waker(&wakers[i])),
            );
            assert_eq!(Arc::strong_count(&counts[i]), 2);
        }
        // These manual polls are all in one task. Let Tokio reset its
        // cooperative budget between cycles, rather than mistaking a forced
        // scheduler yield for socket backpressure.
        tokio::task::yield_now().await;
    }
    socket.close().await;
    Ok(())
}

#[tokio::test]
async fn async_send_variants_wake_independently() -> TestResult {
    let sink = std::net::UdpSocket::bind("127.0.0.1:0")?;
    let socket = Arc::new(UdpSocket::bind_local_v4(0)?);
    socket.connect(sink.local_addr()?)?;
    let sender = socket.clone().create_sender();
    let transmit = test_transmit(sink.local_addr()?);
    let mut sends: [Pin<Box<dyn Future<Output = io::Result<()>>>>; 3] = [
        Box::pin(sender.send(&transmit)),
        Box::pin(async {
            socket
                .send_to(transmit.contents, transmit.destination)
                .await
                .map(|_| ())
        }),
        Box::pin(async { socket.send(transmit.contents).await.map(|_| ()) }),
    ];
    let (counts, wakers) = wake_counters::<3>();
    for i in 0..3 {
        assert!(
            sends[i]
                .as_mut()
                .poll(&mut Context::from_waker(&wakers[i]))
                .is_pending()
        );
    }
    tokio::time::sleep(Duration::from_millis(2)).await;
    for i in 0..3 {
        assert!(counts[i].0.load(Ordering::SeqCst) > 0);
        assert_send_ready(sends[i].as_mut().poll(&mut Context::from_waker(&wakers[i])));
    }
    socket.close().await;
    Ok(())
}

#[tokio::test]
async fn raw_poll_does_not_replace_sender_wakes() -> TestResult {
    let sink = std::net::UdpSocket::bind("127.0.0.1:0")?;
    let socket = Arc::new(UdpSocket::bind_local_v4(0)?);
    let mut sender = socket.clone().create_sender();
    let transmit = test_transmit(sink.local_addr()?);
    let (counts, wakers) = wake_counters::<2>();
    assert!(
        Pin::new(&mut sender)
            .poll_send(&transmit, &mut Context::from_waker(&wakers[0]))
            .is_pending()
    );
    assert!(
        socket
            .poll_writable(&mut Context::from_waker(&wakers[1]))
            .is_pending()
    );
    tokio::time::sleep(Duration::from_millis(2)).await;
    for count in counts {
        assert!(count.0.load(Ordering::SeqCst) > 0);
    }
    socket.close().await;
    Ok(())
}

#[test]
fn send_types_remain_send_sync_unpin() {
    fn check<T: Send + Sync + Unpin>() {}
    check::<UdpSender>();
    check::<SendFut<'static, 'static>>();
    check::<SendToFut<'static, 'static>>();
    check::<SendFutNoq<'static, 'static>>();
}
