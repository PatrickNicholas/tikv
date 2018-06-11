use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use raft::eraftpb::MessageType;
use super::cluster::{Cluster, Simulator};
use super::node::new_node_cluster;
use super::server::new_server_cluster;
use super::transport_simulate::*;
use super::util::*;

fn test_prevote<T: Simulator>(cluster: &mut Cluster<T>, prevote_enabled: bool) {
    cluster.cfg.raft_store.prevote = prevote_enabled;
    cluster.run();

    let ready_notify = Arc::from(AtomicBool::new(true));
    let (notify_tx, notify_rx) = mpsc::channel();
    cluster.sim.write().unwrap().add_send_filter(
        1,
        Box::new(MessageTypeNotifier::new(
            MessageType::MsgRequestPreVoteResponse,
            notify_tx,
            Arc::clone(&ready_notify),
        )),
    );

    let recieved = notify_rx.recv_timeout(Duration::from_secs(5));
    assert_eq!(
        recieved.is_ok(),
        prevote_enabled
    );

    // Restart to ensure prevote is done still.
    // Drain the channel, so we don't have stale messages, and that the cluster is stable.
    while notify_rx.try_recv().is_ok() { continue; }
    assert!(notify_rx.try_recv().is_err());

    // Test restarting a node.
    cluster.must_transfer_leader(1, new_peer(1, 1));
    
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.must_remove_peer(1, new_peer(1, 1));
    
    cluster.add_send_filter(IsolationFilterFactory::new(1));

    let recieved = notify_rx.recv_timeout(Duration::from_secs(5));
    assert_eq!(
        recieved.is_ok(),
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
