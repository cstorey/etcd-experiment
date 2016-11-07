#[macro_use]
extern crate futures;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate futures_cpupool;
extern crate tokio_core;
extern crate tokio_service;
extern crate tokio_timer;
extern crate etcd;
#[macro_use]
extern crate error_chain;

use futures::{Future, Async, Poll};
use futures::stream::Stream;
use tokio_core::reactor::Core;
use tokio_timer::{Timer, Sleep};
use futures_cpupool::{CpuPool, CpuFuture};
use std::sync::Arc;
use std::time::Duration;
use std::fmt;

error_chain! {
    foreign_links {
        etcd::Error, Etcd;
    }
}

enum HeartBeatState {
    New,
    Creating(CpuFuture<etcd::KeySpaceInfo, Error>),
    Started(String, u64),
    Sleeping(String, u64, Sleep),
}
struct HeartBeater {
    pool: CpuPool,
    etcd: Arc<etcd::Client>,
    timer: Timer,
    state: HeartBeatState,
}

enum WatcherState {
    Fresh(Option<u64>),
    Waiting(CpuFuture<etcd::KeySpaceInfo, Error>),
}

#[derive(Debug, Clone)]
enum WatchEvent {
    Alive(String, u64, String),
    Dead(String, u64),
}

impl fmt::Debug for WatcherState {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &WatcherState::Fresh(ref var) => {
                fmt.debug_tuple("WatcherState::Fresh").field(&var).finish()
            }
            &WatcherState::Waiting(_) => writeln!(fmt, "WatcherState::Waiting(_)"),
        }
    }
}


struct Watcher {
    pool: CpuPool,
    etcd: Arc<etcd::Client>,
    timer: Timer,
    state: WatcherState,
}

impl HeartBeater {
    fn new(cpupool: CpuPool, etcd: etcd::Client) -> Self {
        HeartBeater {
            pool: cpupool,
            etcd: Arc::new(etcd),
            state: HeartBeatState::New,
            timer: Timer::default(),
        }
    }

    fn create_if_needed(&mut self) -> Poll<(), Error> {
        if let &mut HeartBeatState::New = &mut self.state {
            // ...
        } else {
            return Ok(Async::Ready(()));
        }

        let create_fut = {
            let cl = self.etcd.clone();
            self.pool.spawn(futures::lazy(move || {
                info!("Starting; creating seq node");
                let res = try!(cl.create_in_order("/", "Hi", Some(5))
                    .map_err(|mut es| es.pop().unwrap()));
                debug!("Created:{:?}", res);
                Ok(res)
            }))
        };

        self.state = HeartBeatState::Creating(create_fut);
        debug!("Creating");
        Ok(Async::Ready(()))
    }

    fn await_setup(&mut self) -> Poll<(), Error> {
        let res = if let &mut HeartBeatState::Creating(ref mut fut) = &mut self.state {
            try_ready!(fut.poll())
        } else {
            return Ok(Async::Ready(()));
        };

        let node = res.node.expect("node");
        let key = node.key.expect("key");
        let ver = node.modified_index.expect("key");

        info!("Alive! {}@{}", key, ver);
        self.state = HeartBeatState::Started(key, ver);
        Ok(Async::Ready(()))
    }
    fn maybe_sleep(&mut self) -> Poll<(), Error> {
        let (key, ver) = if let &mut HeartBeatState::Started(ref key, ref ver) = &mut self.state {
            (key.clone(), ver.clone())
        } else {
            return Ok(Async::Ready(()));
        };

        let sleeper = self.timer.sleep(Duration::from_millis(200));

        self.state = HeartBeatState::Sleeping(key, ver, sleeper);
        Ok(Async::Ready(()))
    }
    fn maybe_send_hb(&mut self) -> Poll<(), Error> {
        let (key, ver) = if let &mut HeartBeatState::Sleeping(ref key, ref ver, ref mut fut) =
                                &mut self.state {
            try_ready!(fut.poll().chain_err(|| "sleeping error?"));
            (key.clone(), ver.clone())
        } else {
            return Ok(Async::Ready(()));
        };

        info!("Ping?");
        let ping_fut = {
            let cl = self.etcd.clone();
            self.pool.spawn(futures::lazy(move || {
                info!("Pinging");
                let res = try!(cl.compare_and_swap(&key, "Hi", Some(5), None, Some(ver))
                    .map_err(|mut es| es.pop().unwrap()));
                debug!("Pinged:{:?}", res);
                Ok(res)
            }))
        };
        self.state = HeartBeatState::Creating(ping_fut);
        Ok(Async::Ready(()))
    }
}


impl Future for HeartBeater {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<(), Error> {
        loop {
            try_ready!(self.create_if_needed());
            try_ready!(self.await_setup());
            try_ready!(self.maybe_send_hb());
            try_ready!(self.maybe_sleep());
        }
    }
}

impl Watcher {
    fn new(cpupool: CpuPool, etcd: etcd::Client) -> Self {
        Watcher {
            pool: cpupool,
            etcd: Arc::new(etcd),
            timer: Timer::default(),
            state: WatcherState::Fresh(None),
        }
    }

    fn maybe_watch(&mut self) -> Poll<(), Error> {
        debug!("maybe_watch? {:?}", self.state);
        let vers = if let &mut WatcherState::Fresh(vers) = &mut self.state {
            debug!("Fresh:{:?}", vers);
            vers
        } else {
            return Ok(Async::Ready(()));
        };

        let watch_fut = {
            let cl = self.etcd.clone();
            self.pool.spawn(futures::lazy(move || {
                let next = vers.map(|v| v+1);
                info!("Watching from {:?}", next);
                let res = try!(cl.watch("/", next, true)
                    .map_err(|mut es| es.pop().unwrap()));
                trace!("Watch Event:{:?}", res);
                Ok(res)
            }))
        };

        self.state = WatcherState::Waiting(watch_fut);
        Ok(Async::Ready(()))
    }
    fn maybe_fetch_result(&mut self) -> Poll<WatchEvent, Error> {
        debug!("maybe_fetch_result? {:?}", self.state);
        let res = if let &mut WatcherState::Waiting(ref mut fut) = &mut self.state {
            debug!("Waiting");
            try_ready!(fut.poll())
        } else {
            return Ok(Async::NotReady);
        };

        trace!("Response: {:?}", res);
        let node = res.clone().node.expect("node");
        let key = node.key.expect("key");
        let ver = node.modified_index.expect("ver");
        let value = node.value;

        self.state = WatcherState::Fresh(Some(ver));
        let event = match value {
            Some(val) => WatchEvent::Alive(key, ver, val),
            None => WatchEvent::Dead(key, ver),
        };

        Ok(Async::Ready(event))
    }
}


impl Stream for Watcher {
    type Item = WatchEvent;
    type Error = Error;
    fn poll(&mut self) -> Poll<Option<Self::Item>, Error> {
        debug!("Watcher#poll");
        try_ready!(self.maybe_watch());
        let val = try_ready!(self.maybe_fetch_result());
        Ok(Async::Ready(Some(val)))
    }
}

fn main() {
    env_logger::init().expect("env_logger");
    let mut core = Core::new().expect("core::new");
    let pool = CpuPool::new(4);
    let hb = HeartBeater::new(pool.clone(), etcd::Client::default());

    info!("Start heartbeater");
    core.handle().spawn(hb.map_err(|e| panic!("etcd heartbeater:{:?}", e)));

    let watcher = Watcher::new(pool.clone(), etcd::Client::default());
    info!("Starting watcher");
    core.handle().spawn(watcher.for_each(|e| Ok(println!("Saw: {:?}", e)))
        .map_err(|e| panic!("etcd heartbeater:{:?}", e)));

    info!("Run");
    core.run(futures::empty::<(), ()>()).expect("run");

}
