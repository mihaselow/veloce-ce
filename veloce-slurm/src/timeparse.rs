//! Slurm `--time` / `-t` → walltime seconds.

use anyhow::{bail, Result};

/// Parse a Slurm time limit into seconds.
///
/// Accepted forms: `minutes`, `minutes:seconds`, `hours:minutes:seconds`,
/// `days-hours`, `days-hours:minutes`, `days-hours:minutes:seconds`.
pub fn parse_slurm_time(raw: &str) -> Result<u64> {
    let s = raw.trim();
    if s.is_empty() {
        bail!("empty time value");
    }
    let (days, rest) = if let Some((d, r)) = s.split_once('-') {
        (parse_u64(d, "days")?, r)
    } else {
        (0, s)
    };
    let parts: Vec<&str> = rest.split(':').collect();
    let (hours, minutes, seconds) = match parts.as_slice() {
        [m] if days == 0 => (0, parse_u64(m, "minutes")?, 0),
        [m] => (parse_u64(m, "hours")?, 0, 0),
        [m, sec] if days == 0 => (0, parse_u64(m, "minutes")?, parse_u64(sec, "seconds")?),
        [h, m] => (parse_u64(h, "hours")?, parse_u64(m, "minutes")?, 0),
        [h, m, sec] => (
            parse_u64(h, "hours")?,
            parse_u64(m, "minutes")?,
            parse_u64(sec, "seconds")?,
        ),
        _ => bail!("invalid time value '{raw}'"),
    };
    Ok(days
        .saturating_mul(86_400)
        .saturating_add(hours.saturating_mul(3600))
        .saturating_add(minutes.saturating_mul(60))
        .saturating_add(seconds))
}

fn parse_u64(s: &str, field: &str) -> Result<u64> {
    s.parse::<u64>()
        .map_err(|_| anyhow::anyhow!("invalid {field} in time value '{s}'"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minutes_only() {
        assert_eq!(parse_slurm_time("10").unwrap(), 600);
    }

    #[test]
    fn hms() {
        assert_eq!(parse_slurm_time("1:10:30").unwrap(), 4230);
    }

    #[test]
    fn days_hours() {
        assert_eq!(parse_slurm_time("1-2").unwrap(), 86_400 + 7200);
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_slurm_time("nope").is_err());
        assert!(parse_slurm_time("1:2:3:4").is_err());
    }
}
