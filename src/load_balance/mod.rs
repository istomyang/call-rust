//! load balance for call-rust, includes:
//! 1. Round Robin, RR
//! 2. Weighted Round Robin, WRR
//! 3. Least Connections, LC
//! 4. Weighted Least Connections, WLC
//! 5. Source IP Hashing, SIPH
//! 6. Random
//! 7. Response Time, RT
//! 8. Power of Two Choices, P2C

use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    sync::RwLock,
    time::{self, SystemTime},
    vec,
};

use anyhow::{Ok, Result};
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

// ==== Trait LoadBalance ====

pub trait LoadBalance {
    fn init(&mut self) -> Result<()>;
    fn nonblocking_run(&mut self) -> Result<()>;
    fn close(&mut self);

    fn new_service_node(&mut self, svc: NewService) -> Result<()>;
    fn get_service(&self, req: Request) -> Result<Response>;
}

pub struct Request {
    pub msg_use_algo: ALGO,
    pub service_name: String,
    pub addr: String,
    pub addr_hash_aux: String, // auxiliary for SIPH
}

pub struct Response {
    pub service_name: String,
    pub service_number: String,
}

pub struct NewService {
    pub name: String,
    pub number: String,
    pub test_rt_url: Option<String>,
    pub weighted: u8,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ALGO {
    None,
    /// Round Robin
    RR,
    /// Weighted Round Robin
    WRR,
    /// Least Connections
    LC,
    /// Weighted Least Connections
    WLC,
    /// Source IP Hashing
    SIPH,
    /// Random
    Random,
    /// Response Time
    RT,
    /// Power of Two Choices
    P2C,
}

impl TryFrom<String> for ALGO {
    type Error = anyhow::Error;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        match value.as_str() {
            "RR" => Ok(ALGO::RR),
            "WRR" => Ok(ALGO::WRR),
            "LC" => Ok(ALGO::LC),
            "WLC" => Ok(ALGO::WLC),
            "SIPH" => Ok(ALGO::SIPH),
            "Random" => Ok(ALGO::Random),
            "RT" => Ok(ALGO::RT),
            "P2C" => Ok(ALGO::P2C),
            "" => Ok(ALGO::None),
            _ => Err(anyhow::anyhow!("invalid algo: {}", value)),
        }
    }
}

struct Service {
    service_name: String,
    nodes: RwLock<Vec<ServiceNode>>,

    pre_service_idx: RwLock<INDEX>, // easy for round robin query.
}

struct ServiceNode {
    number: String,
    weighted: u8,
    weighted_cloned: bool,
    alive_request_count: u32,
    rt: i32,
    rt_test_url: Option<String>,
}

// ==== Trait Algo ====

pub trait Algo {
    fn with_round_robin(pre_idx: INDEX, len: usize) -> INDEX;
    fn with_weighted_round_robin(pre_idx: INDEX, len: usize) -> INDEX;
    fn with_least_connections(s: Vec<u32>) -> INDEX;
    fn with_weighted_least_connections(s: Vec<u32>) -> INDEX;
    fn with_source_ip_hashing(ip: String, len: usize) -> INDEX;
    fn with_random(len: usize) -> INDEX;
    fn with_rt(s: Vec<NoClonedAlgoService>) -> INDEX;
    fn with_p2c(s: Vec<NoClonedAlgoService>) -> INDEX;
}

/// services vector index.
pub type INDEX = usize;

pub struct NoClonedAlgoService {
    pub idx: INDEX,
    pub rt: i32,
    pub alive_request_count: u32,
}

// ==== Load Balance ====

pub struct LoadBalancer {
    services: Vec<Service>,
    rt_test: SystemTime,

    rt_test_tx: Option<oneshot::Sender<Vec<RTSample>>>,
    rt_test_rx: Option<oneshot::Receiver<Vec<RTSample>>>,

    closed: bool,
}

impl LoadBalancer {
    pub fn new() -> Self {
        let (tx, rx) = oneshot::channel();
        LoadBalancer {
            services: vec![],
            rt_test: SystemTime::now() - time::Duration::from_secs(60 * 60),
            rt_test_tx: Some(tx),
            rt_test_rx: Some(rx),
            closed: false,
        }
    }

    async fn run_rt_test(samples: Vec<RTSample>) -> Vec<RTSample> {
        let len = samples.len();
        let (tx, mut rx) = mpsc::channel(16);

        for s in samples {
            let mut s = s;
            let tx_clone = tx.clone();
            tokio::spawn(async move {
                s.run().await;
                tx_clone.send(s).await.unwrap();
            });
        }

        let mut r = vec![];

        loop {
            match rx.try_recv() {
                Result::Ok(s) => r.push(s),
                Err(_) => { /* go on */ }
            }

            if r.len() == len {
                break;
            }
        }

        r
    }
}

impl LoadBalance for LoadBalancer {
    fn init(&mut self) -> Result<()> {
        Ok(())
    }

    fn nonblocking_run(&mut self) -> Result<()> {
        // run rt test
        if !self.closed {
            if self.rt_test.elapsed().unwrap().as_secs() > 5 * 60 {
                let mut samples = vec![];
                for s in &self.services {
                    for svc in s.nodes.read().unwrap().iter() {
                        if let Some(url) = &svc.rt_test_url {
                            samples.push(RTSample {
                                url: url.clone(),
                                result: 0,
                            });
                        }
                    }
                }
                let tx = self.rt_test_tx.take().unwrap();
                tokio::spawn(async move {
                    let r = Self::run_rt_test(samples).await;
                    let _ = tx.send(r);
                });
            }

            match self.rt_test_rx.take().unwrap().try_recv() {
                Result::Ok(results) => {
                    let mut ma = HashMap::new();
                    for r in results {
                        let _ = ma.insert(r.url, r.result);
                    }

                    for s in self.services.iter() {
                        for svc in s.nodes.write().unwrap().iter_mut() {
                            if svc.rt_test_url.is_none() {
                                continue;
                            }
                            if let Some(r) = ma.get(svc.rt_test_url.as_ref().unwrap()) {
                                svc.rt = *r;
                            }
                        }
                    }
                }
                _ => { /* go on */ }
            }
        }

        Ok(())
    }

    fn new_service_node(&mut self, svc: NewService) -> Result<()> {
        let node = ServiceNode {
            number: svc.number,
            weighted: svc.weighted,
            weighted_cloned: false,
            alive_request_count: 0,
            rt: 0,
            rt_test_url: svc.test_rt_url,
        };

        if let Some(a) = self
            .services
            .iter_mut()
            .find(|s| s.service_name == svc.name)
        {
            a.nodes.write().unwrap().push(node);
        } else {
            self.services.push(Service {
                service_name: svc.name,
                nodes: RwLock::new(vec![node]),
                pre_service_idx: RwLock::new(0),
            });
        }
        Ok(())
    }

    fn get_service(&self, req: Request) -> Result<Response> {
        let a = self
            .services
            .iter()
            .find(|s| s.service_name == req.service_name)
            .unwrap();
        a.get_service(req)
    }

    fn close(&mut self) {
        self.closed = true;
    }
}

struct RTSample {
    url: String,
    result: i32, // ms
}

impl RTSample {
    async fn run(&mut self) {
        let t0 = std::time::Instant::now();
        match reqwest::get(&self.url).await {
            Result::Ok(_) => {
                let t1 = std::time::Instant::now();
                let elapsed: i32 = (t1 - t0).as_millis().try_into().unwrap();
                self.result = elapsed;
            }
            Err(_) => {
                self.result = -1;
            }
        }
    }
}

// ==== Impl Service ====

impl Service {
    fn get_service(&self, req: Request) -> Result<Response> {
        let r = match req.msg_use_algo {
            ALGO::None => Ok(Response {
                service_name: req.service_name,
                service_number: self.nodes.read().unwrap()[0].number.clone(),
            }),
            ALGO::RR => {
                let idx = self.get_idx();
                let len = self.get_len_no_cloned();
                let a = AlgoImpl::with_round_robin(idx, len);
                let no = self.get_service_no(a);
                Ok(Response {
                    service_name: req.service_name,
                    service_number: no,
                })
            }
            ALGO::WRR => {
                let idx = self.get_idx();
                let len = self.get_len();
                let a = AlgoImpl::with_weighted_round_robin(idx, len);
                let no = self.get_service_no(a);
                Ok(Response {
                    service_name: self.service_name.clone(),
                    service_number: no,
                })
            }
            ALGO::LC => {
                let s = self
                    .nodes
                    .read()
                    .unwrap()
                    .iter()
                    .filter(|s| !s.weighted_cloned)
                    .map(|s| s.alive_request_count)
                    .collect::<Vec<u32>>();
                let a = AlgoImpl::with_least_connections(s);
                let no = self.get_service_no(a);
                Ok(Response {
                    service_name: req.service_name,
                    service_number: no,
                })
            }
            ALGO::WLC => {
                let s = self
                    .nodes
                    .read()
                    .unwrap()
                    .iter()
                    .filter(|s| !s.weighted_cloned)
                    .map(|s| s.alive_request_count / (s.weighted as u32))
                    .collect::<Vec<u32>>();
                let a = AlgoImpl::with_weighted_least_connections(s);
                let no = self.get_service_no(a);
                Ok(Response {
                    service_name: self.service_name.clone(),
                    service_number: no,
                })
            }
            ALGO::SIPH => {
                let ip = req.addr + req.addr_hash_aux.as_str();
                let len = self.nodes.read().unwrap().len();
                let idx = AlgoImpl::with_source_ip_hashing(ip, len);
                let no = self.get_service_no(idx);
                Ok(Response {
                    service_name: req.service_name,
                    service_number: no,
                })
            }
            ALGO::Random => {
                let len = self.nodes.read().unwrap().len();
                let idx = AlgoImpl::with_random(len);
                let no = self.get_service_no(idx);
                Ok(Response {
                    service_name: req.service_name,
                    service_number: no,
                })
            }
            ALGO::RT => {
                let s = self
                    .nodes
                    .read()
                    .unwrap()
                    .iter()
                    .filter(|s| !s.weighted_cloned)
                    .enumerate()
                    .map(|(idx, s)| NoClonedAlgoService {
                        idx,
                        rt: s.rt,
                        alive_request_count: s.alive_request_count,
                    })
                    .collect::<Vec<NoClonedAlgoService>>();
                let idx = AlgoImpl::with_rt(s);
                let no = self.get_service_no(idx);
                Ok(Response {
                    service_name: req.service_name,
                    service_number: no,
                })
            }
            ALGO::P2C => {
                let s = self
                    .nodes
                    .read()
                    .unwrap()
                    .iter()
                    .filter(|s| !s.weighted_cloned)
                    .enumerate()
                    .map(|(idx, s)| NoClonedAlgoService {
                        idx,
                        rt: s.rt,
                        alive_request_count: s.alive_request_count,
                    })
                    .collect::<Vec<NoClonedAlgoService>>();
                let idx = AlgoImpl::with_p2c(s);
                let no = self.get_service_no(idx);
                Ok(Response {
                    service_name: self.service_name.clone(),
                    service_number: no,
                })
            }
        };

        {
            let winner_number = r.as_ref().unwrap().service_number.clone();
            let mut w = self.nodes.write().unwrap();
            w.iter_mut()
                .find(|s| s.number == winner_number)
                .map(|s| s.alive_request_count += 1);
        }

        r.map_err(anyhow::Error::from)
    }

    fn get_service_no(&self, idx: INDEX) -> String {
        self.nodes.read().unwrap()[idx].number.clone()
    }

    fn get_idx(&self) -> usize {
        *self.pre_service_idx.read().unwrap()
    }

    fn get_len_no_cloned(&self) -> usize {
        self.nodes
            .read()
            .unwrap()
            .iter()
            .filter(|s| !s.weighted_cloned)
            .count()
    }

    fn get_len(&self) -> usize {
        self.nodes.read().unwrap().len()
    }
}

// ==== Impl Algo ====

struct AlgoImpl;

impl AlgoImpl {
    fn inner_with_least_connections(s: Vec<u32>) -> INDEX {
        let mut smallest = s[0];
        let mut smallest_idx = 0;
        for (i, s) in s.iter().enumerate() {
            let s = s.clone();
            if s < smallest {
                smallest = s;
                smallest_idx = i;
            }
        }
        smallest_idx
    }
}

impl Algo for AlgoImpl {
    fn with_round_robin(pre_idx: INDEX, len: usize) -> INDEX {
        (pre_idx + 1) % len
    }

    fn with_weighted_round_robin(pre_idx: INDEX, len: usize) -> INDEX {
        (pre_idx + 1) % len
    }

    fn with_least_connections(s: Vec<u32>) -> INDEX {
        Self::inner_with_least_connections(s)
    }

    fn with_weighted_least_connections(s: Vec<u32>) -> INDEX {
        Self::inner_with_least_connections(s)
    }

    fn with_source_ip_hashing(ip: String, len: usize) -> INDEX {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        ip.hash(&mut hasher);
        let hash = hasher.finish() as usize;
        hash % len
    }

    fn with_random(len: usize) -> INDEX {
        let mut rng = rand::thread_rng();
        rng.gen_range(0..len)
    }

    fn with_rt(s: Vec<NoClonedAlgoService>) -> INDEX {
        let mut smallest = s[0].rt;
        let mut smallest_idx = 0;
        for (i, s) in s.iter().enumerate() {
            let s = s.rt.clone();
            if s < smallest {
                smallest = s;
                smallest_idx = i;
            }
        }
        smallest_idx
    }

    fn with_p2c(s: Vec<NoClonedAlgoService>) -> INDEX {
        let mut rng = rand::thread_rng();
        let len = s.len();
        let a = &s[rng.gen_range(0..len)];
        let b = &s[rng.gen_range(0..len)];
        if a.alive_request_count < b.alive_request_count {
            a.idx
        } else {
            b.idx
        }
    }
}
