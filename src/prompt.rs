//! The prompt that primes the engine: the call's own vocabulary.

use crate::config::PromptConfig;
use trunk_recorder_plugin::CallRecord;

/// The longest prompt, in characters.
const MAX_CHARS: usize = 200;

/// Build the engine prompt for `record`; empty when prompts are disabled.
pub fn build(record: &CallRecord, system_name: &str, cfg: &PromptConfig) -> String {
    if !cfg.enabled {
        return String::new();
    }
    let mut tags: Vec<&str> = Vec::new();
    for tag in record.src_list.iter().map(|s| s.tag.as_str()) {
        if !tag.is_empty() && !tags.contains(&tag) {
            tags.push(tag);
        }
    }
    let units = if tags.is_empty() { String::new() } else { format!("Units: {}", tags.join(", ")) };
    let parts = [record.talkgroup_tag.as_str(), record.talkgroup_description.as_str(), system_name, units.as_str(), cfg.extra.as_str()];
    let joined = parts.into_iter().filter(|p| !p.is_empty()).collect::<Vec<_>>().join(". ");
    cut(&joined)
}

/// Cut `s` to at most `MAX_CHARS` characters without splitting a word.
fn cut(s: &str) -> String {
    if s.chars().count() <= MAX_CHARS {
        return s.to_string();
    }
    // Byte offset of the character at index MAX_CHARS: the exclusive end of
    // the window, so a space exactly at index MAX_CHARS ends the last word.
    let end = s.char_indices().nth(MAX_CHARS).map_or(s.len(), |(i, _)| i);
    let window = &s[..end];
    let kept = match window.rfind(' ') {
        Some(i) => &window[..i],
        None => &window[..window.char_indices().nth(MAX_CHARS).map_or(window.len(), |(i, _)| i)],
    };
    kept.trim_end_matches(' ').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use trunk_recorder_plugin::SrcEntry;

    fn src(id: i64, tag: &str) -> SrcEntry {
        SrcEntry { src: id, tag: tag.into(), ..Default::default() }
    }

    #[test]
    fn the_prompt_is_the_vocabulary_capped_at_200() {
        let r = CallRecord {
            talkgroup_tag: "Fire Dispatch".into(),
            talkgroup_description: "County fire main".into(),
            src_list: vec![src(101, "Engine 3"), src(102, ""), src(101, "Engine 3")],
            ..Default::default()
        };
        let cfg = PromptConfig { enabled: true, extra: "Middletown".into() };
        assert_eq!(build(&r, "OHMARCS", &cfg), "Fire Dispatch. County fire main. OHMARCS. Units: Engine 3. Middletown");
        assert_eq!(build(&r, "", &PromptConfig { enabled: false, extra: "x".into() }), "");
        let long = CallRecord { talkgroup_description: "word ".repeat(60), ..Default::default() };
        let p = build(&long, "", &cfg);
        assert!(p.len() <= 200 && !p.ends_with(' ') && p.ends_with("word"));
    }

    #[test]
    fn units_are_left_out_when_no_radio_has_a_tag() {
        let r = CallRecord { talkgroup_tag: "Dispatch".into(), src_list: vec![src(1, "")], ..Default::default() };
        assert_eq!(build(&r, "", &PromptConfig::default()), "Dispatch");
    }

    #[test]
    fn a_single_long_word_is_cut_at_200_characters() {
        let r = CallRecord { talkgroup_tag: "\u{e9}".repeat(300), ..Default::default() };
        let p = build(&r, "", &PromptConfig::default());
        assert_eq!(p.chars().count(), 200);
    }
}
