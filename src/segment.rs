//! Splitting a transcript into segments at the radios' boundaries.

use trunk_recorder_plugin::SrcEntry;

/// One word with its time in the audio, as the engine reports it.
#[derive(Debug, Clone, PartialEq, serde::Deserialize)]
pub struct Word {
    pub word: String,
    pub start: f64,
    pub end: f64,
}

/// A stretch of the transcript spoken by one radio.
#[derive(Debug, Clone, PartialEq)]
pub struct Segment {
    pub unit: Option<i64>,
    pub start_s: f64,
    pub end_s: f64,
    pub text: String,
}

/// Split `text` into segments, one per radio that spoke.
///
/// `words` carry the timing; without them the whole `text` is one segment.
/// A word belongs to the radio whose transmission began last before the
/// word's midpoint (the first radio, for words before any of them).
pub fn split(words: &[Word], text: &str, src_list: &[SrcEntry], length_s: f64) -> Vec<Segment> {
    if words.is_empty() {
        if text.is_empty() {
            return Vec::new();
        }
        return vec![Segment { unit: None, start_s: 0.0, end_s: length_s, text: text.to_string() }];
    }
    if src_list.is_empty() {
        return vec![span(None, words)];
    }
    let mut bounds: Vec<(i64, f64)> = src_list.iter().map(|s| (s.src, s.pos)).collect();
    bounds.sort_by(|a, b| a.1.total_cmp(&b.1));
    let mut groups: Vec<Vec<Word>> = vec![Vec::new(); bounds.len()];
    for word in words {
        let mid = (word.start + word.end) / 2.0;
        let i = bounds.partition_point(|b| b.1 <= mid).saturating_sub(1);
        groups[i].push(word.clone());
    }
    bounds.iter().zip(&groups).filter(|(_, g)| !g.is_empty()).map(|(b, g)| span(Some(b.0), g)).collect()
}

/// One segment spanning non-empty `words`.
fn span(unit: Option<i64>, words: &[Word]) -> Segment {
    Segment { unit, start_s: words[0].start, end_s: words[words.len() - 1].end, text: words.iter().map(|w| w.word.as_str()).collect::<Vec<_>>().join(" ") }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn w(t: &str, s: f64, e: f64) -> Word {
        Word { word: t.into(), start: s, end: e }
    }

    fn src(id: i64, pos: f64) -> SrcEntry {
        SrcEntry { src: id, pos, ..Default::default() }
    }

    #[test]
    fn words_fall_under_the_radio_talking() {
        let words = [w("tower", 0.2, 0.5), w("seventy", 0.6, 0.9), w("three", 1.0, 1.3), w("go", 4.1, 4.3), w("ahead", 4.4, 4.8)];
        let segs = split(&words, "tower seventy three go ahead", &[src(1001, 0.0), src(1002, 4.0)], 6.0);
        assert_eq!(segs.len(), 2);
        assert_eq!((segs[0].unit, segs[0].start_s, segs[0].end_s, segs[0].text.as_str()), (Some(1001), 0.2, 1.3, "tower seventy three"));
        assert_eq!((segs[1].unit, segs[1].text.as_str()), (Some(1002), "go ahead"));
    }

    #[test]
    fn one_radio_no_timestamps_and_no_radios() {
        let one = split(&[], "all of it", &[src(7, 0.0)], 3.0);
        assert_eq!((one[0].unit, one[0].start_s, one[0].end_s, one[0].text.as_str()), (None, 0.0, 3.0, "all of it"));
        assert!(split(&[], "", &[src(7, 0.0)], 3.0).is_empty());
        let none = split(&[w("hi", 0.1, 0.4)], "hi", &[], 2.0);
        assert_eq!((none[0].unit, none[0].text.as_str()), (None, "hi"));
        // Overlapping radios: a word's midpoint decides; a radio that said nothing has no segment.
        let segs = split(&[w("a", 0.0, 1.0), w("b", 1.2, 1.4)], "a b", &[src(1, 0.0), src(2, 0.9), src(3, 1.1)], 2.0);
        assert_eq!(segs.iter().map(|s| s.unit).collect::<Vec<_>>(), [Some(1), Some(3)]);
    }

    #[test]
    fn words_before_the_first_radio_join_it_and_unsorted_radios_are_sorted() {
        let segs = split(&[w("x", 0.0, 0.2), w("y", 3.0, 3.2)], "x y", &[src(2, 2.0), src(1, 1.0)], 4.0);
        assert_eq!(segs.iter().map(|s| (s.unit, s.text.as_str())).collect::<Vec<_>>(), [(Some(1), "x"), (Some(2), "y")]);
    }
}
