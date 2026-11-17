//! Node 受管进程适配，脚本与敏感消息只经过标准输入输出

use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use gateway_admin::ports::javascript::{
    JavaScriptError, JavaScriptRuntime, JavaScriptSession, MAX_JAVASCRIPT_MESSAGE_BYTES,
};
use secrecy::{ExposeSecret as _, SecretString};
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};

use super::{ProcessControl, ProcessSpec, ProcessStartError, ProcessSupervisor};

const BOOTSTRAP: &str = r#"const lines=require('readline').createInterface({input:process.stdin});lines.once('line',source=>{const module={exports:{}};new Function('require','module','exports','__dirname',JSON.parse(source))(require,module,module.exports,process.cwd());module.exports(lines);});"#;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct JavaScriptConfig {
    pub executable: PathBuf,
}

impl Default for JavaScriptConfig {
    fn default() -> Self {
        Self {
            executable: PathBuf::from("node"),
        }
    }
}

impl JavaScriptConfig {
    pub fn resolve_and_validate(&mut self, source_dir: &Path) -> Result<(), crate::ConfigError> {
        if self.executable.as_os_str().is_empty() {
            return Err(crate::ConfigError::InvalidField(
                "host.javascript.executable",
            ));
        }
        if self.executable.is_relative() && self.executable.components().count() > 1 {
            self.executable = source_dir.join(&self.executable);
        }
        Ok(())
    }
}

pub struct NodeJavaScriptRuntime {
    executable: PathBuf,
    directory: PathBuf,
    processes: Arc<ProcessSupervisor>,
}

impl NodeJavaScriptRuntime {
    #[must_use]
    pub fn new(
        config: JavaScriptConfig,
        directory: PathBuf,
        processes: Arc<ProcessSupervisor>,
    ) -> Self {
        // 在清空子进程环境前解析 PATH，避免依赖 Node 的安装目录是否属于系统默认搜索路径
        let executable = if config.executable.is_relative()
            && config.executable.components().count() == 1
        {
            let resolved = std::env::var_os("PATH")
                .into_iter()
                .flat_map(|paths| std::env::split_paths(&paths).collect::<Vec<_>>())
                .map(|path| {
                    let path = path.join(&config.executable);
                    if !std::env::consts::EXE_EXTENSION.is_empty() && path.extension().is_none() {
                        path.with_extension(std::env::consts::EXE_EXTENSION)
                    } else {
                        path
                    }
                })
                .find(|path| path.is_file());
            resolved.unwrap_or(config.executable)
        } else {
            config.executable
        };
        Self {
            executable,
            directory,
            processes,
        }
    }
}

#[async_trait]
impl JavaScriptRuntime for NodeJavaScriptRuntime {
    async fn start(
        &self,
        program: &str,
        timezone: &str,
    ) -> Result<Box<dyn JavaScriptSession>, JavaScriptError> {
        let program = SecretString::from(
            serde_json::to_string(program).map_err(|_| JavaScriptError::Communication)?,
        );
        if program.expose_secret().len() > MAX_JAVASCRIPT_MESSAGE_BYTES {
            return Err(JavaScriptError::MessageLimit);
        }
        let mut environment = vec![(OsString::from("TZ"), OsString::from(timezone))];
        // Windows 加密随机数初始化依赖 SystemRoot，不能随普通部署环境一起清空
        if cfg!(windows)
            && let Some(system_root) = std::env::var_os("SystemRoot")
        {
            environment.push((OsString::from("SystemRoot"), system_root));
        }
        let process = self
            .processes
            .spawn(ProcessSpec {
                executable: self.executable.clone(),
                arguments: vec![OsString::from("--eval"), OsString::from(BOOTSTRAP)],
                environment,
                directory: self.directory.clone(),
                maximum_stderr_bytes: 64 * 1024,
            })
            .map_err(|error| match error {
                ProcessStartError::Capacity => JavaScriptError::Capacity,
                ProcessStartError::Spawn { .. } => JavaScriptError::Unavailable,
                ProcessStartError::Pipes => JavaScriptError::Communication,
            })?;
        let mut session = Box::new(NodeSession {
            input: process.input,
            output: BufReader::new(process.output),
            control: process.control,
        });
        session.send(&program).await?;
        Ok(session)
    }
}

struct NodeSession {
    input: tokio::process::ChildStdin,
    output: BufReader<tokio::process::ChildStdout>,
    control: ProcessControl,
}

#[async_trait]
impl JavaScriptSession for NodeSession {
    async fn send(&mut self, message: &SecretString) -> Result<(), JavaScriptError> {
        let message = message.expose_secret();
        if message.len() > MAX_JAVASCRIPT_MESSAGE_BYTES
            || message.contains('\n')
            || message.contains('\r')
        {
            return Err(JavaScriptError::MessageLimit);
        }
        self.input
            .write_all(message.as_bytes())
            .await
            .map_err(|_| JavaScriptError::Communication)?;
        self.input
            .write_all(b"\n")
            .await
            .map_err(|_| JavaScriptError::Communication)?;
        self.input
            .flush()
            .await
            .map_err(|_| JavaScriptError::Communication)
    }

    async fn receive(&mut self) -> Result<SecretString, JavaScriptError> {
        let mut bytes = Vec::new();
        let count = (&mut self.output)
            .take((MAX_JAVASCRIPT_MESSAGE_BYTES + 1) as u64)
            .read_until(b'\n', &mut bytes)
            .await
            .map_err(|_| JavaScriptError::Communication)?;
        if count == 0 {
            return Err(JavaScriptError::Closed);
        }
        if count > MAX_JAVASCRIPT_MESSAGE_BYTES {
            return Err(JavaScriptError::MessageLimit);
        }
        String::from_utf8(bytes)
            .map(SecretString::from)
            .map_err(|_| JavaScriptError::Communication)
    }

    async fn close(self: Box<Self>) {
        self.control.stop();
        self.control.exited().await;
    }
}

impl Drop for NodeSession {
    fn drop(&mut self) {
        self.control.stop();
    }
}
