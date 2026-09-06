use serde::{Deserialize, Serialize};

/// Swarm envelope carried inside the message body (upper-layer protocol).
/// Wire form is `---swarm\n<yaml>\n---\n<markdown>` (see PROTOCOL.md).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmHeader {
    pub task_id: String,
    pub from: String,
    #[serde(default)]
    pub reply_to: String,
    #[serde(default = "default_attempt")]
    pub attempt: u32,
}

fn default_attempt() -> u32 {
    1
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmMessage {
    pub header: SwarmHeader,
    pub payload: String,
}

pub fn parse(text: &str) -> Option<SwarmMessage> {
    let rest = text.strip_prefix("---swarm\n")?;
    let end = rest.find("\n---\n").or_else(|| rest.strip_suffix("\n---").map(|s| s.len()))?;
    let head_raw = &rest[..end];
    let mut payload = &rest[end..];
    payload = payload.strip_prefix("\n---\n").unwrap_or(payload);
    let mut header: SwarmHeader = serde_yaml_safe_parse(head_raw)?;
    if header.task_id.trim().is_empty() {
        return None;
    }
    if uuid::Uuid::parse_str(header.task_id.trim()).is_err() {
        return None;
    }
    header.task_id = header.task_id.trim().to_string();
    if header.from.trim().is_empty() {
        header.from = ".".into();
    }
    Some(SwarmMessage {
        header,
        payload: payload.to_string(),
    })
}

fn serde_yaml_safe_parse(raw: &str) -> Option<SwarmHeader> {
    // Minimal YAML-subset parser: `key: value` lines only. No new dependency.
    let mut task_id = None;
    let mut from = None;
    let mut reply_to = String::new();
    let mut attempt = 1u32;
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (k, v) = line.split_once(':')?;
        let v = v.trim().trim_matches('"').trim_matches('\'').to_string();
        match k.trim() {
            "task_id" => task_id = Some(v),
            "from" => from = Some(v),
            "reply_to" => reply_to = v,
            "attempt" => attempt = v.parse().unwrap_or(1),
            _ => {}
        }
    }
    Some(SwarmHeader {
        task_id: task_id?,
        from: from.unwrap_or_else(|| ".".into()),
        reply_to,
        attempt,
    })
}

pub fn render(header: &SwarmHeader, role: &str, payload_markdown: &str) -> String {
    let mut s = String::from("---swarm\n");
    s.push_str(&format!("task_id: {}\n", header.task_id));
    s.push_str(&format!("from: {}\n", header.from));
    s.push_str(&format!("reply_to: {}\n", header.reply_to));
    s.push_str(&format!("attempt: {}\n", header.attempt));
    s.push_str("---\n");
    if !role.is_empty() {
        s.push_str(&format!("## role: {}\n\n{}\n", role, role));
    }
    s.push_str(payload_markdown);
    if !payload_markdown.ends_with('\n') {
        s.push('\n');
    }
    s
}

pub fn failed_payload(reason: &str, body: &str) -> String {
    format!("> swarm-failed: {reason}\n\n{body}")
}

pub fn cancelled_payload(reason: &str, body: &str) -> String {
    format!("> swarm-cancelled: {reason}\n\n{body}")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn roundtrip() {
        let h = SwarmHeader {
            task_id: uuid::Uuid::new_v4().to_string(),
            from: "planner".into(),
            reply_to: String::new(),
            attempt: 1,
        };
        let text = render(&h, "planner role", "do X");
        let m = parse(&text).unwrap();
        assert_eq!(m.header, h);
        assert!(m.payload.contains("do X"));
    }

    #[test]
    fn non_swarm_body_is_none() {
        assert!(parse("hello").is_none());
        assert!(parse("---swarm\ntask_id: not-a-uuid\n---\n x").is_none());
        assert!(parse("---swarm\nno fields\n---\n x").is_none());
    }

    #[test]
    fn missing_tail_is_tolerated() {
        let id = uuid::Uuid::new_v4().to_string();
        let m = parse(&format!("---swarm\ntask_id: {id}\nfrom: a\n---")).unwrap();
        assert_eq!(m.header.task_id, id);
    }
}
