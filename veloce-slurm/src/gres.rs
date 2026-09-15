//! Honor `gpu:N` / `name:count`. Skip typed GRES such as `gpu:mi300x:8`.

use anyhow::{bail, Result};

use crate::Skip;

pub type GresParse = (Vec<(String, u64)>, Vec<Skip>);

/// Parse a Slurm `--gres` value into honored pairs and skips.
pub fn parse_gres(raw: &str) -> Result<GresParse> {
    let mut honored = Vec::new();
    let mut skips = Vec::new();
    for piece in raw.split(',') {
        let piece = piece.trim();
        if piece.is_empty() {
            continue;
        }
        let parts: Vec<&str> = piece.split(':').collect();
        match parts.as_slice() {
            [name, count] => {
                let n = parse_count(count, piece)?;
                honored.push(((*name).to_string(), n));
            }
            [name, kind, count] => {
                let _ = parse_count(count, piece)?;
                skips.push(Skip {
                    option: format!("--gres={piece}"),
                    reason: format!(
                        "typed GRES '{name}:{kind}' is not a Veloce GRES key; use --gres {name}:N"
                    ),
                });
            }
            [count] if count.chars().all(|c| c.is_ascii_digit()) => {
                honored.push(("gpu".to_string(), parse_count(count, piece)?));
            }
            _ => bail!("invalid --gres value '{piece}'"),
        }
    }
    Ok((honored, skips))
}

fn parse_count(s: &str, piece: &str) -> Result<u64> {
    s.parse::<u64>()
        .map_err(|_| anyhow::anyhow!("invalid --gres count in '{piece}'"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gpu_count() {
        let (h, s) = parse_gres("gpu:2").unwrap();
        assert_eq!(h, vec![("gpu".to_string(), 2)]);
        assert!(s.is_empty());
    }

    #[test]
    fn typed_skipped() {
        let (h, s) = parse_gres("gpu:mi300x:8").unwrap();
        assert!(h.is_empty());
        assert_eq!(s.len(), 1);
        assert!(s[0].option.contains("mi300x"));
    }
}
