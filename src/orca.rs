use anyhow::Context;
use serde_json::Value;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;

/// Blocking single-request client for swarm.sock (used by CLI + TUI bootstrap).
pub fn client_request(sock: &Path, req: Value) -> anyhow::Result<Value> {
    let mut s = UnixStream::connect(sock)
        .with_context(|| format!("connect {}", sock.display()))?;
    let mut line = serde_json::to_string(&req)?;
    line.push('\n');
    s.write_all(line.as_bytes())?;
    let mut r = BufReader::new(&s);
    let mut out = String::new();
    r.read_line(&mut out)?;
    Ok(serde_json::from_str(&out).with_context(|| "parse swarm.sock response")?)
}
