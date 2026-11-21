//! Small helpers for sanitising anything that came from a remote peer
//! before it touches the filesystem or the terminal.

/// Maximum length (in bytes) of a file name we will ever create.
pub const MAX_FILE_NAME_BYTES: usize = 255;

/// Maximum length (in chars) of a peer display name.
pub const MAX_DISPLAY_NAME_CHARS: usize = 32;

const RESERVED_WINDOWS_NAMES: [&str; 22] = [
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
    "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

/// Turn an untrusted name into a safe single path component.
///
/// - Only the last path component is kept (so `../../etc/passwd` -> `passwd`).
/// - Control characters and characters that are illegal on common filesystems
///   are replaced with `_`.
/// - Leading/trailing spaces and dots are trimmed.
/// - Empty / dot-only names become `file`.
/// - Windows reserved device names get a `_` prefix.
/// - The result is truncated to [`MAX_FILE_NAME_BYTES`] on a char boundary.
pub fn sanitize_file_name(raw: &str) -> String {
    let last = raw.rsplit(['/', '\\']).next().unwrap_or("");

    let cleaned: String = last
        .chars()
        .map(|c| {
            if c.is_control() || matches!(c, '<' | '>' | ':' | '"' | '|' | '?' | '*') {
                '_'
            } else {
                c
            }
        })
        .collect();

    let trimmed = cleaned.trim_matches(|c| c == ' ' || c == '.');
    let mut name = if trimmed.is_empty() {
        "file".to_string()
    } else {
        trimmed.to_string()
    };

    let stem = name.split('.').next().unwrap_or("").to_ascii_uppercase();
    if RESERVED_WINDOWS_NAMES.contains(&stem.as_str()) {
        name.insert(0, '_');
    }

    truncate_to_bytes(&mut name, MAX_FILE_NAME_BYTES);
    name
}

/// Sanitise a name that will only be *displayed* (peer names, offered file
/// names). Strips control characters so a remote peer cannot inject terminal
/// escape sequences, and caps the length.
pub fn sanitize_display(raw: &str) -> String {
    sanitize_display_max(raw, MAX_DISPLAY_NAME_CHARS)
}

/// Like [`sanitize_display`] with a custom length cap (used for file names).
pub fn sanitize_display_max(raw: &str, max_chars: usize) -> String {
    let cleaned: String = raw
        .chars()
        .filter(|c| !c.is_control())
        .take(max_chars)
        .collect();
    let cleaned = cleaned.trim().to_string();
    if cleaned.is_empty() {
        "Unknown".to_string()
    } else {
        cleaned
    }
}

fn truncate_to_bytes(s: &mut String, max: usize) {
    if s.len() <= max {
        return;
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    s.truncate(end);
}

/// Human-readable byte count, e.g. `1.50 MiB`.
pub fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = n as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} {}", n, UNITS[0])
    } else {
        format!("{:.2} {}", value, UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_path_traversal() {
        assert_eq!(sanitize_file_name("../../etc/passwd"), "passwd");
        assert_eq!(sanitize_file_name("..\\..\\windows\\evil.dll"), "evil.dll");
        assert_eq!(sanitize_file_name("/abs/path/file.txt"), "file.txt");
    }

    #[test]
    fn handles_degenerate_names() {
        assert_eq!(sanitize_file_name(""), "file");
        assert_eq!(sanitize_file_name(".."), "file");
        assert_eq!(sanitize_file_name("..."), "file");
        assert_eq!(sanitize_file_name("a/b/"), "file");
    }

    #[test]
    fn replaces_bad_chars_and_reserved_names() {
        assert_eq!(sanitize_file_name("a:b*c?.txt"), "a_b_c_.txt");
        assert_eq!(sanitize_file_name("CON.txt"), "_CON.txt");
        assert_eq!(sanitize_file_name("bell\u{7}.txt"), "bell_.txt");
    }

    #[test]
    fn truncates_on_char_boundary() {
        let long = "é".repeat(300);
        let out = sanitize_file_name(&long);
        assert!(out.len() <= MAX_FILE_NAME_BYTES);
        assert!(out.chars().all(|c| c == 'é'));
    }

    #[test]
    fn display_names_lose_control_chars() {
        assert_eq!(sanitize_display("al\u{1b}[31mice"), "al[31mice");
        assert_eq!(sanitize_display("\u{1b}"), "Unknown");
        assert_eq!(sanitize_display(&"x".repeat(100)).chars().count(), 32);
    }

    #[test]
    fn human_bytes_formats() {
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(1536), "1.50 KiB");
        assert_eq!(human_bytes(5 * 1024 * 1024), "5.00 MiB");
    }
}
