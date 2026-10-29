//! The gate: cheap checks that decide whether a call is worth sending to an engine.

use crate::audio::{Pcm, rms};
use crate::config::GateConfig;
use trunk_recorder_plugin::CallRecord;

/// Why a call should not be transcribed, or `None` to go ahead.
///
/// Order: encrypted, too short, silent. The RMS check applies only when
/// decoded audio is available.
pub fn check(record: &CallRecord, pcm: Option<&Pcm>, cfg: &GateConfig) -> Option<&'static str> {
    if record.encrypted && cfg.skip_encrypted {
        return Some("encrypted");
    }
    if record.call_length < cfg.min_seconds {
        return Some("too short");
    }
    if pcm.is_some_and(|p| rms(&p.samples) < cfg.rms) {
        return Some("silent");
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audio::sine;

    #[test]
    fn encrypted_then_short_then_silent() {
        let cfg = GateConfig::default();
        let mut r = CallRecord { call_length: 5.0, encrypted: true, ..Default::default() };
        assert_eq!(check(&r, None, &cfg), Some("encrypted"));
        r.encrypted = false;
        r.call_length = 0.5;
        assert_eq!(check(&r, None, &cfg), Some("too short"));
        r.call_length = 5.0;
        let quiet = Pcm { rate: 8000, samples: vec![0.002; 8000] };
        assert_eq!(check(&r, Some(&quiet), &cfg), Some("silent"));
        let loud = Pcm { rate: 8000, samples: sine(8000, 1000.0, 1.0, 0.3) };
        assert_eq!(check(&r, Some(&loud), &cfg), None);
        assert_eq!(check(&r, None, &cfg), None, "an M4A has no RMS gate");
        let keep = GateConfig { skip_encrypted: false, ..cfg };
        r.encrypted = true;
        assert_eq!(check(&r, None, &keep), None);
    }
}
