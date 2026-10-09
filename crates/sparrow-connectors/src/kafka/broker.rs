//! Isolated single-node KRaft Kafka broker for the opt-in tests.
//!
//! `SPARROW_KAFKA_HOME` (extracted Apache Kafka 4.3.1) and
//! `SPARROW_JAVA_HOME` (Temurin 21 JRE) come from `scripts/kafka-broker.sh`,
//! which pins and checksum-verifies both. Each sandbox owns a temp data
//! directory and loopback ports; nothing external is touched.

#![allow(dead_code)]

use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

// `super::rdkafka` so the control-plane tests can include this file by path.
use super::rdkafka::admin::{AdminClient, AdminOptions, NewPartitions, NewTopic, TopicReplication};
use super::rdkafka::client::DefaultClientContext;
use super::rdkafka::config::ClientConfig;
use super::rdkafka::producer::{BaseProducer, Producer};

const CLUSTER_ID: &str = "q1Sh-9_ISia_zwGINzRvyQ";

pub struct KafkaSandbox {
    pub port: u16,
    controller_port: u16,
    dir: PathBuf,
    child: Option<Child>,
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn env(name: &str) -> PathBuf {
    PathBuf::from(std::env::var(name).unwrap_or_else(|_| {
        panic!("{name} not set; run scripts/kafka-broker.sh and export its output")
    }))
}

impl KafkaSandbox {
    pub fn start() -> Self {
        let root = std::env::var_os("SPARROW_TEST_ARTIFACTS")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let dir = root.join(format!(
            "sparrow-kafka-{}-{}",
            std::process::id(),
            free_port()
        ));
        std::fs::create_dir_all(dir.join("data")).unwrap();
        let mut sandbox = Self {
            port: free_port(),
            controller_port: free_port(),
            dir,
            child: None,
        };
        sandbox.write_config();
        let status = Command::new(env("SPARROW_KAFKA_HOME").join("bin/kafka-storage.sh"))
            .args(["format", "-t", CLUSTER_ID, "-c"])
            .arg(sandbox.dir.join("server.properties"))
            .envs(sandbox.java_env())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert!(status.success(), "kafka-storage format failed");
        sandbox.spawn();
        sandbox
    }

    pub fn bootstrap(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    fn write_config(&self) {
        let (b, c) = (self.port, self.controller_port);
        let config = format!(
            "process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:{c}
listeners=PLAINTEXT://127.0.0.1:{b},CONTROLLER://127.0.0.1:{c}
advertised.listeners=PLAINTEXT://127.0.0.1:{b}
controller.listener.names=CONTROLLER
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs={data}
num.partitions=1
auto.create.topics.enable=false
offsets.topic.replication.factor=1
offsets.topic.num.partitions=4
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
share.coordinator.state.topic.replication.factor=1
share.coordinator.state.topic.min.isr=1
group.initial.rebalance.delay.ms=0
group.min.session.timeout.ms=6000
",
            data = self.dir.join("data").display()
        );
        std::fs::write(self.dir.join("server.properties"), config).unwrap();
    }

    fn java_env(&self) -> Vec<(&'static str, String)> {
        vec![
            ("JAVA_HOME", env("SPARROW_JAVA_HOME").display().to_string()),
            ("KAFKA_HEAP_OPTS", "-Xms256m -Xmx512m".into()),
            ("LOG_DIR", self.dir.join("logs").display().to_string()),
        ]
    }

    fn spawn(&mut self) {
        let child = Command::new(env("SPARROW_KAFKA_HOME").join("bin/kafka-server-start.sh"))
            .arg(self.dir.join("server.properties"))
            .envs(self.java_env())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        self.child = Some(child);
        self.wait_ready();
    }

    fn wait_ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(60);
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", self.bootstrap())
            .set("log_level", "0")
            .create()
            .unwrap();
        loop {
            if let Ok(m) = producer
                .client()
                .fetch_metadata(None, Duration::from_secs(2))
            {
                if !m.brokers().is_empty() {
                    return;
                }
            }
            assert!(Instant::now() < deadline, "Kafka broker did not start");
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    /// Graceful stop (SIGTERM) and wait.
    pub fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = Command::new("kill")
                .args(["-TERM", &child.id().to_string()])
                .status();
            let deadline = Instant::now() + Duration::from_secs(30);
            while child.try_wait().unwrap().is_none() {
                if Instant::now() > deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    break;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }

    /// Restart on the same ports and data directory.
    pub fn restart(&mut self) {
        self.stop();
        self.spawn();
    }

    fn admin(&self) -> AdminClient<DefaultClientContext> {
        ClientConfig::new()
            .set("bootstrap.servers", self.bootstrap())
            .create()
            .unwrap()
    }

    pub async fn create_topic(&self, name: &str, partitions: i32) {
        let admin = self.admin();
        let r = admin
            .create_topics(
                &[NewTopic::new(name, partitions, TopicReplication::Fixed(1))],
                &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
            )
            .await
            .unwrap();
        for t in r {
            t.unwrap();
        }
        // Wait until every partition has a leader.
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", self.bootstrap())
            .create()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let ready = producer
                .client()
                .fetch_metadata(Some(name), Duration::from_secs(2))
                .ok()
                .is_some_and(|m| {
                    m.topics().iter().any(|t| {
                        t.error().is_none()
                            && t.partitions().len() == partitions as usize
                            && t.partitions().iter().all(|p| p.leader() >= 0)
                    })
                });
            if ready {
                return;
            }
            assert!(Instant::now() < deadline, "topic {name} not ready");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn add_partitions(&self, name: &str, total: usize) {
        let r = self
            .admin()
            .create_partitions(
                &[NewPartitions::new(name, total)],
                &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
            )
            .await
            .unwrap();
        for t in r {
            t.unwrap();
        }
    }
}

impl Drop for KafkaSandbox {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if std::env::var_os("SPARROW_TEST_ARTIFACTS").is_none() && !std::thread::panicking() {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}
