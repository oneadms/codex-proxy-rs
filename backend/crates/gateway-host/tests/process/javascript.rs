//! 验证受管 JavaScript 的环境、消息限制、取消与进程容量回收

use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use gateway_admin::ports::javascript::{
    JavaScriptError, JavaScriptRuntime, MAX_JAVASCRIPT_MESSAGE_BYTES,
};
use gateway_host::process::{NodeJavaScriptRuntime, ProcessSupervisor};
use secrecy::{ExposeSecret as _, SecretString};

#[tokio::test]
async fn javascript_environment_and_close_preserve_process_capacity() {
    let directory = tempfile::tempdir().unwrap();
    let processes = Arc::new(ProcessSupervisor::new(NonZeroUsize::new(1).unwrap()));
    let runtime =
        NodeJavaScriptRuntime::new(Default::default(), directory.path().to_owned(), processes);
    let program = r#"module.exports=lines=>lines.on('line',line=>process.stdout.write(JSON.stringify({
        value:JSON.parse(line), timezone:Intl.DateTimeFormat().resolvedOptions().timeZone,
        hasPath:process.env.PATH!==undefined, random:require('crypto').randomUUID().length
    })+'\n'));"#;
    let mut session = runtime.start(program, "Asia/Tokyo").await.unwrap();
    assert_eq!(
        runtime.start(program, "Asia/Tokyo").await.err(),
        Some(JavaScriptError::Capacity)
    );
    let oversized = SecretString::from("x".repeat(MAX_JAVASCRIPT_MESSAGE_BYTES + 1));
    assert_eq!(
        session.send(&oversized).await,
        Err(JavaScriptError::MessageLimit)
    );
    session
        .send(&SecretString::from("\"synthetic-private\"".to_owned()))
        .await
        .unwrap();
    let response = session.receive().await.unwrap();
    let response: serde_json::Value = serde_json::from_str(response.expose_secret()).unwrap();
    assert_eq!(response["value"], "synthetic-private");
    assert_eq!(response["timezone"], "Asia/Tokyo");
    assert_eq!(response["hasPath"], false);
    assert_eq!(response["random"], 36);
    session.close().await;
    let replacement = runtime.start(program, "Asia/Tokyo").await.unwrap();
    replacement.close().await;
}

#[tokio::test]
async fn javascript_output_is_bounded_and_missing_executable_does_not_keep_a_slot() {
    let directory = tempfile::tempdir().unwrap();
    let processes = Arc::new(ProcessSupervisor::new(NonZeroUsize::new(1).unwrap()));
    let missing = NodeJavaScriptRuntime::new(
        gateway_host::process::JavaScriptConfig {
            executable: directory.path().join("private-missing-runtime"),
        },
        directory.path().to_owned(),
        processes.clone(),
    );
    let error = missing
        .start("module.exports=()=>{};", "UTC")
        .await
        .err()
        .unwrap();
    assert_eq!(error, JavaScriptError::Unavailable);
    assert!(!format!("{error:?} {error}").contains("private-missing-runtime"));
    let runtime =
        NodeJavaScriptRuntime::new(Default::default(), directory.path().to_owned(), processes);
    let program = format!(
        "module.exports=()=>process.stdout.write('x'.repeat({}));",
        MAX_JAVASCRIPT_MESSAGE_BYTES + 1
    );
    let mut session = runtime.start(&program, "UTC").await.unwrap();
    let response = tokio::time::timeout(Duration::from_secs(5), session.receive())
        .await
        .unwrap();
    assert_eq!(response.err(), Some(JavaScriptError::MessageLimit));
    session.close().await;
}

#[tokio::test]
async fn cancelling_a_javascript_wait_reclaims_the_child_and_its_slot() {
    let directory = tempfile::tempdir().unwrap();
    let processes = Arc::new(ProcessSupervisor::new(NonZeroUsize::new(1).unwrap()));
    let runtime =
        NodeJavaScriptRuntime::new(Default::default(), directory.path().to_owned(), processes);
    let program = r#"module.exports=()=>process.stdout.write('"ready"\n');"#;
    let mut session = runtime.start(program, "UTC").await.unwrap();
    assert_eq!(
        session.receive().await.unwrap().expose_secret(),
        "\"ready\"\n"
    );
    let result = tokio::time::timeout(
        Duration::from_millis(25),
        async move { session.receive().await },
    )
    .await;
    assert!(result.is_err());
    let replacement = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match runtime.start(program, "UTC").await {
                Ok(session) => break session,
                Err(JavaScriptError::Capacity) => tokio::task::yield_now().await,
                Err(error) => panic!("unexpected runtime error: {error:?}"),
            }
        }
    })
    .await
    .unwrap();
    replacement.close().await;
}
