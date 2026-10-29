//! The text filters: reject transcripts that are empty, implausibly fast,
//! stuck in a loop, or a known engine hallucination.

use crate::config::FilterConfig;

/// The outcome of filtering one transcript.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Verdict {
    Ok,
    Empty,
    Rejected(&'static str),
}

/// Lowercase, with ASCII punctuation stripped from both ends.
fn normalise(token: &str) -> String {
    token.trim_matches(|c: char| c.is_ascii_punctuation()).to_lowercase()
}

/// Filter `text`, which was spoken over `length_s` seconds.
///
/// Words are whitespace tokens. The rate check is skipped when `length_s <= 0`.
/// Repetition is the same normalised token more than `max_repeat` times in a
/// row. A hallucination is the whole normalised text equal to a list entry.
pub fn check(text: &str, length_s: f64, cfg: &FilterConfig) -> Verdict {
    let tokens: Vec<String> = text.split_whitespace().map(normalise).collect();
    if tokens.is_empty() {
        return Verdict::Empty;
    }
    if length_s > 0.0 && tokens.len() as f64 / length_s > cfg.max_words_per_second {
        return Verdict::Rejected("implausible rate");
    }
    let mut run = 0;
    let mut previous: Option<&String> = None;
    for token in &tokens {
        run = if previous == Some(token) { run + 1 } else { 1 };
        if run > cfg.max_repeat {
            return Verdict::Rejected("repetition");
        }
        previous = Some(token);
    }
    let whole = tokens.join(" ");
    let is_known = cfg.hallucinations.iter().any(|h| {
        let entry: Vec<String> = h.split_whitespace().map(normalise).collect();
        entry.join(" ") == whole
    });
    if is_known {
        return Verdict::Rejected("hallucination");
    }
    Verdict::Ok
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_four_verdicts() {
        let cfg = FilterConfig::default();
        assert_eq!(check("", 5.0, &cfg), Verdict::Empty);
        assert_eq!(check("   \n", 5.0, &cfg), Verdict::Empty);
        assert_eq!(check("Tower 73 go ahead", 4.0, &cfg), Verdict::Ok);
        let fast = "one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen seventeen";
        assert_eq!(check(fast, 2.0, &cfg), Verdict::Rejected("implausible rate"));
        assert_eq!(check(fast, 0.0, &cfg), Verdict::Ok, "no length, no rate check");
        assert_eq!(check("ok ok ok ok ok done", 3.0, &cfg), Verdict::Rejected("repetition"));
        assert_eq!(check("ok ok ok ok done", 3.0, &cfg), Verdict::Ok);
        assert_eq!(check("Thank you for watching.", 3.0, &cfg), Verdict::Rejected("hallucination"));
        assert_eq!(check("thank you for watching!", 3.0, &cfg), Verdict::Rejected("hallucination"));
        assert_eq!(check("Thank you for watching the scene", 3.0, &cfg), Verdict::Ok, "only the whole text");
    }
}
