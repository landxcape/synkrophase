use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;
use uuid::Uuid;

use synkrophase::cli::handlers::common::perform_join_handshake;
use synkrophase::controller::mock::MockMediaController;
use synkrophase::protocol::messages::{QueueState, Role};
use synkrophase::session::SessionState;
use synkrophase::session::runtime::SessionMessageRuntime;

#[tokio::test]
async fn test_join_handshake_success() {
    let host_id = Uuid::new_v4();
    let host_session = Arc::new(SessionState::new_leader(
        "ROOMX".into(),
        host_id,
        "HostDevice".into(),
    ));

    let host_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let host_addr: SocketAddr = host_socket.local_addr().unwrap();

    let host_controller = Arc::new(MockMediaController::new());
    let host_runtime = SessionMessageRuntime::new(Arc::clone(&host_session), "HostDevice".into())
        .with_controller(host_controller);

    let host_task = tokio::spawn({
        let sock = Arc::clone(&host_socket);
        async move { host_runtime.run_receive_loop(sock).await }
    });

    let follower_id = Uuid::new_v4();
    let follower_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());

    let follower_session = Arc::new(SessionState::from_join(
        "ROOMX".into(),
        follower_id,
        "FollowerDevice".into(),
        host_id,
        host_addr,
        vec![],
        QueueState::default(),
    ));

    assert!(!follower_session.is_join_confirmed());

    let follower_controller = Arc::new(MockMediaController::new());
    let follower_runtime =
        SessionMessageRuntime::new(Arc::clone(&follower_session), "FollowerDevice".into())
            .with_controller(follower_controller);

    let follower_task = tokio::spawn({
        let sock = Arc::clone(&follower_socket);
        async move { follower_runtime.run_receive_loop(sock).await }
    });

    // Run handshake with 5 attempts, 100ms interval
    let handshake_res = perform_join_handshake(
        &follower_socket,
        follower_id,
        host_addr,
        "ROOMX".into(),
        "FollowerDevice".into(),
        &follower_session,
        5,
        Duration::from_millis(100),
    )
    .await;

    assert!(
        handshake_res.is_ok(),
        "Handshake should succeed: {:?}",
        handshake_res
    );
    assert!(follower_session.is_join_confirmed());
    assert_eq!(follower_session.leader_id(), host_id);
    assert_eq!(follower_session.role(), Role::Moderator);

    host_task.abort();
    follower_task.abort();
}

#[tokio::test]
async fn test_join_handshake_timeout_when_host_unresponsive() {
    let dummy_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let dummy_addr: SocketAddr = dummy_socket.local_addr().unwrap();
    drop(dummy_socket); // Port is now closed

    let follower_id = Uuid::new_v4();
    let follower_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());

    let follower_session = Arc::new(SessionState::from_join(
        "ROOMX".into(),
        follower_id,
        "FollowerDevice".into(),
        Uuid::new_v4(),
        dummy_addr,
        vec![],
        QueueState::default(),
    ));

    assert!(!follower_session.is_join_confirmed());

    // Run handshake with 3 attempts, 50ms interval (fast test)
    let handshake_res = perform_join_handshake(
        &follower_socket,
        follower_id,
        dummy_addr,
        "ROOMX".into(),
        "FollowerDevice".into(),
        &follower_session,
        3,
        Duration::from_millis(50),
    )
    .await;

    assert!(handshake_res.is_err(), "Handshake should timeout");
    assert!(
        !follower_session.is_join_confirmed(),
        "Join must remain unconfirmed"
    );
    assert!(
        !follower_session.is_leader(),
        "Unconfirmed joiner must not become leader"
    );
}
