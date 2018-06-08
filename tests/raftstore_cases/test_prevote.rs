use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;

use raft::eraftpb::MessageType;

use super::cluster::{Cluster, Simulator};
use super::node::new_node_cluster;
use super::server::new_server_cluster;
use super::transport_simulate::*;

fn test_prevote<T: Simulator>(cluster: &mut Cluster<T>, prevote_enabled: bool) {
    cluster.cfg.raft_store.prevote = prevote_enabled;
    cluster.run();

    let ready_notify = Arc::default();
    let (notify_tx, notify_rx) = mpsc::channel();
    cluster.sim.write().unwrap().add_send_filter(
        1,
        Box::new(MessageTypeNotifier::new(
            MessageType::MsgRequestPreVote,
            notify_tx,
            Arc::clone(&ready_notify),
        )),
    );
    ready_notify.store(true, Ordering::SeqCst);

    assert_eq!(
        notify_rx.recv_timeout(Duration::from_secs(3)).is_ok(),
        prevote_enabled
    );
}

#[test]
fn test_node_prevote() {
    let mut cluster = new_node_cluster(0, 3);
    test_prevote(&mut cluster, true);
}

#[test]
fn test_server_prevote() {
    let mut cluster = new_server_cluster(0, 3);
    test_prevote(&mut cluster, true);
}

#[test]
fn test_node_no_prevote() {
    let mut cluster = new_node_cluster(0, 3);
    test_prevote(&mut cluster, false);
}

#[test]
fn test_server_no_prevote() {
    let mut cluster = new_server_cluster(0, 3);
    test_prevote(&mut cluster, false);
}
