#![cfg(not(target_os = "wasi"))]
#![cfg(all(feature = "os-poll", feature = "net"))]

use mio::{Events, Poll, Token, Waker};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

mod util;
use util::{assert_send, assert_sync, expect_no_events, init};

#[test]
fn is_send_and_sync() {
    assert_send::<Waker>();
    assert_sync::<Waker>();
}

#[test]
fn waker() {
    init();

    let mut poll = Poll::new().expect("unable to create new Poll instance");
    let mut events = Events::with_capacity(10);

    let token = Token(10);
    let waker = Waker::new(poll.registry(), token).expect("unable to create waker");

    waker.wake().expect("unable to wake");
    expect_waker_event(&mut poll, &mut events, token);
}

#[test]
fn waker_multiple_wakeups_same_thread() {
    init();

    let mut poll = Poll::new().expect("unable to create new Poll instance");
    let mut events = Events::with_capacity(10);

    let token = Token(10);
    let waker = Waker::new(poll.registry(), token).expect("unable to create waker");

    for _ in 0..3 {
        waker.wake().expect("unable to wake");
    }
    expect_waker_event(&mut poll, &mut events, token);
}

#[test]
fn waker_wakeup_different_thread() {
    init();

    let mut poll = Poll::new().expect("unable to create new Poll instance");
    let mut events = Events::with_capacity(10);

    let token = Token(10);
    let waker = Waker::new(poll.registry(), token).expect("unable to create waker");

    let waker = Arc::new(waker);
    let waker1 = Arc::clone(&waker);
    let handle = thread::spawn(move || {
        waker1.wake().expect("unable to wake");
    });

    expect_waker_event(&mut poll, &mut events, token);

    expect_no_events(&mut poll, &mut events);

    handle.join().unwrap();
}

#[test]
fn waker_multiple_wakeups_different_thread() {
    init();

    let mut poll = Poll::new().expect("unable to create new Poll instance");
    let mut events = Events::with_capacity(10);

    let token = Token(10);
    let waker = Waker::new(poll.registry(), token).expect("unable to create waker");
    let waker = Arc::new(waker);
    let waker1 = Arc::clone(&waker);
    let waker2 = Arc::clone(&waker1);

    let handle1 = thread::spawn(move || {
        waker1.wake().expect("unable to wake");
    });

    let barrier = Arc::new(Barrier::new(2));
    let barrier2 = barrier.clone();
    let handle2 = thread::spawn(move || {
        barrier2.wait();
        waker2.wake().expect("unable to wake");
    });

    // Receive the event from thread 1.
    expect_waker_event(&mut poll, &mut events, token);

    // Unblock thread 2.
    barrier.wait();

    // Now we need to receive another event from thread 2.
    expect_waker_event(&mut poll, &mut events, token);

    expect_no_events(&mut poll, &mut events);

    handle1.join().unwrap();
    handle2.join().unwrap();
}

#[test]
#[cfg_attr(
    not(debug_assertions),
    ignore = "only works with debug_assertions enabled"
)]
#[should_panic = "Only a single `Waker` can be active per `Poll` instance"]
fn using_multiple_wakers_panics() {
    init();

    let poll = Poll::new().expect("unable to create new Poll instance");
    let token1 = Token(10);
    let token2 = Token(11);

    let waker1 = Waker::new(poll.registry(), token1).expect("unable to first waker");
    // This should panic.
    let waker2 = Waker::new(poll.registry(), token2).unwrap();

    drop(waker1);
    drop(waker2);
}

#[test]
#[cfg_attr(
    not(debug_assertions),
    ignore = "only works with debug_assertions enabled"
)]
#[should_panic = "Only a single `Waker` can be active per `Poll` instance"]
fn using_multiple_wakers_panics_different_cloned_registries() {
    init();

    let poll = Poll::new().expect("unable to create new Poll instance");
    let registry1 = poll.registry().try_clone().unwrap();
    let registry2 = poll.registry().try_clone().unwrap();

    let token1 = Token(10);
    let token2 = Token(11);

    let waker1 = Waker::new(&registry1, token1).expect("unable to first waker");
    // This should panic.
    let waker2 = Waker::new(&registry2, token2).unwrap();

    drop(waker1);
    drop(waker2);
}

/// Regression test: registering or deregistering an fd while another thread
/// calls `Waker::wake` must not swallow the wakeup. The `poll(2)` selector
/// shares one notify pipe between both and used to drain the waker's byte.
#[test]
#[cfg(unix)]
fn waker_wakeup_concurrent_with_registration() {
    use mio::unix::SourceFd;
    use mio::Interest;
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream;

    init();

    let mut poll = Poll::new().expect("unable to create new Poll instance");
    let mut events = Events::with_capacity(10);

    let token = Token(10);
    let waker = Arc::new(Waker::new(poll.registry(), token).expect("unable to create waker"));
    let registry = poll.registry().try_clone().unwrap();
    let (stream, _peer) = UnixStream::pair().unwrap();

    const ROUNDS: usize = 1000;
    // Main thread, waker thread and registering thread.
    let start = Arc::new(Barrier::new(3));
    let done = Arc::new(Barrier::new(3));

    let handle1 = {
        let (start, done, waker) = (start.clone(), done.clone(), waker.clone());
        thread::spawn(move || {
            for _ in 0..ROUNDS {
                start.wait();
                waker.wake().expect("unable to wake");
                done.wait();
            }
        })
    };
    let handle2 = {
        let (start, done) = (start.clone(), done.clone());
        thread::spawn(move || {
            let fd = stream.as_raw_fd();
            for _ in 0..ROUNDS {
                start.wait();
                registry
                    .register(&mut SourceFd(&fd), Token(20), Interest::READABLE)
                    .expect("unable to register");
                registry
                    .deregister(&mut SourceFd(&fd))
                    .expect("unable to deregister");
                done.wait();
            }
        })
    };

    for round in 0..ROUNDS {
        start.wait();
        done.wait();
        // Every wake must be delivered; with the race it never shows up.
        let mut woken = false;
        while !woken {
            poll.poll(&mut events, Some(Duration::from_secs(1))).unwrap();
            assert!(!events.is_empty(), "lost wakeup in round {round}");
            woken = events.iter().any(|event| event.token() == token);
        }
    }

    handle1.join().unwrap();
    handle2.join().unwrap();
}

fn expect_waker_event(poll: &mut Poll, events: &mut Events, token: Token) {
    poll.poll(events, Some(Duration::from_millis(100))).unwrap();
    assert!(!events.is_empty());
    for event in events.iter() {
        assert_eq!(event.token(), token);
        assert!(event.is_readable());
    }
}
