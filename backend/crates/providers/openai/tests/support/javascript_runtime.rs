//! 使用真实受管 Node 运行时验证脚本消息与进程回收

use gateway_admin::ports::javascript::JavaScriptRuntime;
use std::sync::Arc;

pub fn javascript_runtime() -> Arc<dyn JavaScriptRuntime> {
    Arc::new(gateway_host::process::NodeJavaScriptRuntime::new(
        Default::default(),
        std::env::temp_dir(),
        Arc::new(gateway_host::process::ProcessSupervisor::default()),
    ))
}
