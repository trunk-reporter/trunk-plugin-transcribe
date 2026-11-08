//! The gate: cheap checks that decide whether a call is worth sending to an engine.

use crate::audio::{Pcm, rms};
use crate::config::{GateConfig, ListMode};
use trunk_recorder_plugin::CallRecord;

/// Why a call's talkgroup should not be transcribed, or `None` to go ahead:
/// the talkgroup and category lists, checked before the audio is read.
///
/// Both lists apply: a call has to pass the talkgroup list and the category
/// list. An empty list passes every call. A category matches the talkgroup's
/// tag (`talkgroup_group_tag`, e.g. "Law Dispatch") or its group
/// (`talkgroup_group`, e.g. "Fire"), ignoring case and surrounding spaces.
pub fn check_talkgroup(record: &CallRecord, cfg: &GateConfig) -> Option<&'static str> {
    let tgs = &cfg.talkgroups;
    if !tgs.list.is_empty() {
        let listed = tgs.list.contains(&record.talkgroup);
        match tgs.mode {
            ListMode::Allowlist if !listed => return Some("talkgroup not allowed"),
            ListMode::Denylist if listed => return Some("talkgroup denied"),
            _ => {}
        }
    }
    let wanted: Vec<String> = cfg.categories.list.iter().map(|c| c.trim().to_lowercase()).filter(|c| !c.is_empty()).collect();
    if !wanted.is_empty() {
        let have = [record.talkgroup_group_tag.trim().to_lowercase(), record.talkgroup_group.trim().to_lowercase()];
        let listed = have.iter().any(|h| !h.is_empty() && wanted.contains(h));
        match cfg.categories.mode {
            ListMode::Allowlist if !listed => return Some("category not allowed"),
            ListMode::Denylist if listed => return Some("category denied"),
            _ => {}
        }
    }
    None
}

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
    use crate::config::{CategoryList, TalkgroupList};

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

    fn tg_call(talkgroup: u32, tag: &str, group: &str) -> CallRecord {
        CallRecord { talkgroup, talkgroup_group_tag: tag.into(), talkgroup_group: group.into(), ..Default::default() }
    }

    #[test]
    fn empty_lists_pass_every_talkgroup() {
        let cfg = GateConfig::default();
        assert_eq!(check_talkgroup(&tg_call(101, "", ""), &cfg), None);
        let blank = GateConfig { categories: CategoryList { mode: ListMode::Allowlist, list: vec!["  ".into()] }, ..Default::default() };
        assert_eq!(check_talkgroup(&tg_call(101, "", ""), &blank), None, "a blank entry is no entry");
        let empty_allow = GateConfig { talkgroups: TalkgroupList { mode: ListMode::Allowlist, list: vec![] }, ..Default::default() };
        assert_eq!(check_talkgroup(&tg_call(101, "", ""), &empty_allow), None, "an empty allowlist is off, not everything out");
    }

    #[test]
    fn talkgroup_allowlist_and_denylist() {
        let allow = GateConfig { talkgroups: TalkgroupList { mode: ListMode::Allowlist, list: vec![101, 102] }, ..Default::default() };
        assert_eq!(check_talkgroup(&tg_call(101, "", ""), &allow), None);
        assert_eq!(check_talkgroup(&tg_call(201, "", ""), &allow), Some("talkgroup not allowed"));
        let deny = GateConfig { talkgroups: TalkgroupList { mode: ListMode::Denylist, list: vec![201] }, ..Default::default() };
        assert_eq!(check_talkgroup(&tg_call(201, "", ""), &deny), Some("talkgroup denied"));
        assert_eq!(check_talkgroup(&tg_call(101, "", ""), &deny), None);
    }

    #[test]
    fn categories_match_the_tag_or_the_group_ignoring_case() {
        let allow =
            GateConfig { categories: CategoryList { mode: ListMode::Allowlist, list: vec!["Law Dispatch".into(), " fire ".into()] }, ..Default::default() };
        assert_eq!(check_talkgroup(&tg_call(1, "law dispatch", "Police"), &allow), None, "by tag");
        assert_eq!(check_talkgroup(&tg_call(2, "Fire-Tac", "FIRE"), &allow), None, "by group");
        assert_eq!(check_talkgroup(&tg_call(3, "Public Works", "Services"), &allow), Some("category not allowed"));
        assert_eq!(check_talkgroup(&tg_call(4, "", ""), &allow), Some("category not allowed"), "no tag or group: not listed");
        let deny =
            GateConfig { categories: CategoryList { mode: ListMode::Denylist, list: vec!["Public Works".into(), "Hospital".into()] }, ..Default::default() };
        assert_eq!(check_talkgroup(&tg_call(3, "Public Works", "Services"), &deny), Some("category denied"));
        assert_eq!(check_talkgroup(&tg_call(4, "", ""), &deny), None);
    }

    #[test]
    fn both_lists_have_to_pass_talkgroup_first() {
        let cfg = GateConfig {
            talkgroups: TalkgroupList { mode: ListMode::Denylist, list: vec![9] },
            categories: CategoryList { mode: ListMode::Allowlist, list: vec!["Fire Dispatch".into()] },
            ..Default::default()
        };
        assert_eq!(check_talkgroup(&tg_call(9, "Fire Dispatch", ""), &cfg), Some("talkgroup denied"));
        assert_eq!(check_talkgroup(&tg_call(10, "Law Dispatch", ""), &cfg), Some("category not allowed"));
        assert_eq!(check_talkgroup(&tg_call(10, "Fire Dispatch", ""), &cfg), None);
    }
}
