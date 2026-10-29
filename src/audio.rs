//! Audio: WAV in, RMS, resampling to 16 kHz, the P25 clean-up filter, WAV out.

use std::f64::consts::PI;
use std::path::Path;

const TARGET_RATE: u32 = 16_000;
/// Peak level the P25 filter normalises to: -3 dBFS.
const PEAK_TARGET: f64 = 0.707_945_784_384_137_9;
/// Half-width, in input samples, of the 33-tap resampling kernel.
const HALF_TAPS: i64 = 16;

/// Mono audio, samples in -1.0..1.0.
#[derive(Debug, Clone, PartialEq)]
pub struct Pcm {
    pub rate: u32,
    pub samples: Vec<f32>,
}

/// Reads a WAV file of any rate, 8/16/24/32-bit integer or float, averaging channels to mono.
pub fn read_wav(path: &Path) -> Result<Pcm, String> {
    let name = path.display();
    let mut reader = hound::WavReader::open(path).map_err(|e| format!("{name}: {e}"))?;
    let spec = reader.spec();
    let channels = spec.channels.max(1) as usize;
    let interleaved: Vec<f32> = match spec.sample_format {
        hound::SampleFormat::Float => reader.samples::<f32>().collect::<Result<_, _>>().map_err(|e| format!("{name}: {e}"))?,
        hound::SampleFormat::Int => {
            let scale = 1.0 / (1u64 << (spec.bits_per_sample - 1)) as f64;
            reader.samples::<i32>().map(|s| s.map(|v| (v as f64 * scale) as f32)).collect::<Result<_, _>>().map_err(|e| format!("{name}: {e}"))?
        }
    };
    let samples = interleaved.chunks(channels).map(|frame| frame.iter().sum::<f32>() / frame.len() as f32).collect();
    Ok(Pcm { rate: spec.sample_rate, samples })
}

/// Root mean square of the samples; 0.0 for none.
pub fn rms(samples: &[f32]) -> f32 {
    if samples.is_empty() {
        return 0.0;
    }
    let sum: f64 = samples.iter().map(|s| (*s as f64).powi(2)).sum();
    (sum / samples.len() as f64).sqrt() as f32
}

/// Resamples to 16 kHz with a 33-tap Hann-windowed sinc, cut off at 0.45 of the lower rate.
pub fn resample_to_16k(pcm: &Pcm) -> Vec<f32> {
    if pcm.rate == TARGET_RATE {
        return pcm.samples.clone();
    }
    let in_rate = pcm.rate as f64;
    let step = in_rate / TARGET_RATE as f64;
    // Cutoff as a fraction of the input rate.
    let cutoff = 0.45 * in_rate.min(TARGET_RATE as f64) / in_rate;
    let out_len = (pcm.samples.len() as f64 / step).round() as usize;
    let kernel = |d: f64| -> f64 {
        let x = 2.0 * cutoff * d;
        let sinc = if x.abs() < 1e-12 { 1.0 } else { (PI * x).sin() / (PI * x) };
        let hann = 0.5 * (1.0 + (PI * d / (HALF_TAPS as f64 + 1.0)).cos());
        sinc * hann
    };
    (0..out_len)
        .map(|n| {
            let t = n as f64 * step;
            let centre = t.round() as i64; // the 33 nearest input samples
            let (mut acc, mut weights) = (0.0, 0.0);
            for k in centre - HALF_TAPS..=centre + HALF_TAPS {
                let w = kernel(k as f64 - t);
                weights += w;
                if let Some(s) = usize::try_from(k).ok().and_then(|k| pcm.samples.get(k)) {
                    acc += w * *s as f64;
                }
            }
            // Dividing by the weight sum gives unity DC gain, deliberately.
            (acc / weights) as f32
        })
        .collect()
}

/// One biquad section (Audio EQ Cookbook), direct form I.
struct Biquad {
    b: [f64; 3],
    a: [f64; 2],
}

impl Biquad {
    fn new(kind: Kind, hz: f64, q: f64) -> Self {
        let w0 = 2.0 * PI * hz / TARGET_RATE as f64;
        let (sin, cos) = w0.sin_cos();
        let alpha = sin / (2.0 * q);
        let (b0, b1, b2) = match kind {
            Kind::HighPass => ((1.0 + cos) / 2.0, -(1.0 + cos), (1.0 + cos) / 2.0),
            Kind::LowPass => ((1.0 - cos) / 2.0, 1.0 - cos, (1.0 - cos) / 2.0),
            Kind::Notch => (1.0, -2.0 * cos, 1.0),
        };
        let a0 = 1.0 + alpha;
        Biquad { b: [b0 / a0, b1 / a0, b2 / a0], a: [-2.0 * cos / a0, (1.0 - alpha) / a0] }
    }

    fn run(&self, samples: &mut [f32]) {
        let (mut x1, mut x2, mut y1, mut y2) = (0.0, 0.0, 0.0, 0.0);
        for s in samples {
            let x0 = *s as f64;
            let y0 = self.b[0] * x0 + self.b[1] * x1 + self.b[2] * x2 - self.a[0] * y1 - self.a[1] * y2;
            (x2, x1, y2, y1) = (x1, x0, y1, y0);
            *s = y0 as f32;
        }
    }
}

enum Kind {
    HighPass,
    LowPass,
    Notch,
}

/// Cleans up P25 audio at 16 kHz: band-limit to 300-3400 Hz, notch the 1000, 1200 and 1800 Hz
/// signalling tones, then peak-normalise to -3 dBFS (silence stays silent).
pub fn p25_filter(samples: &mut [f32]) {
    const BUTTERWORTH_Q: f64 = std::f64::consts::FRAC_1_SQRT_2;
    let chain = [
        Biquad::new(Kind::HighPass, 300.0, BUTTERWORTH_Q),
        Biquad::new(Kind::LowPass, 3400.0, BUTTERWORTH_Q),
        Biquad::new(Kind::Notch, 1200.0, 5.0),
        Biquad::new(Kind::Notch, 1800.0, 5.0),
        Biquad::new(Kind::Notch, 1000.0, 8.0),
    ];
    for stage in &chain {
        stage.run(samples);
    }
    let peak = samples.iter().fold(0.0f32, |m, s| m.max(s.abs()));
    if peak > 0.0 {
        let gain = PEAK_TARGET as f32 / peak;
        for s in samples {
            *s *= gain;
        }
    }
}

/// A RIFF/WAVE file: 16-bit PCM, mono, 16 000 Hz, 44-byte header.
pub fn wav_16k(samples: &[f32]) -> Vec<u8> {
    let data_len = (samples.len() * 2) as u32;
    let mut out = Vec::with_capacity(44 + data_len as usize);
    out.extend_from_slice(b"RIFF");
    out.extend_from_slice(&(36 + data_len).to_le_bytes());
    out.extend_from_slice(b"WAVEfmt ");
    out.extend_from_slice(&16u32.to_le_bytes());
    out.extend_from_slice(&1u16.to_le_bytes()); // PCM
    out.extend_from_slice(&1u16.to_le_bytes()); // mono
    out.extend_from_slice(&TARGET_RATE.to_le_bytes());
    out.extend_from_slice(&(TARGET_RATE * 2).to_le_bytes()); // byte rate
    out.extend_from_slice(&2u16.to_le_bytes()); // block align
    out.extend_from_slice(&16u16.to_le_bytes());
    out.extend_from_slice(b"data");
    out.extend_from_slice(&data_len.to_le_bytes());
    for s in samples {
        let v = (s.clamp(-1.0, 1.0) * 32767.0).round() as i16;
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

/// One second of 440 Hz at -20 dBFS, as a 16 kHz WAV, for checking an engine is reachable.
pub fn test_tone() -> Vec<u8> {
    let samples: Vec<f32> = (0..TARGET_RATE).map(|n| (0.1 * (2.0 * PI * 440.0 * n as f64 / TARGET_RATE as f64).sin()) as f32).collect();
    wav_16k(&samples)
}

#[cfg(test)]
pub fn sine(rate: u32, hz: f32, secs: f32, amp: f32) -> Vec<f32> {
    use std::f32::consts::PI;
    (0..(rate as f32 * secs) as usize).map(|n| amp * (2.0 * PI * hz * n as f32 / rate as f32).sin()).collect()
}

#[cfg(test)]
pub fn write_temp(bytes: &[u8]) -> std::path::PathBuf {
    use trunk_recorder_plugin::testing;
    let path = testing::temp_dir("audio").join("temp.wav");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap();
    path
}

#[cfg(test)]
mod tests {
    use super::*;
    use trunk_recorder_plugin::testing;

    #[test]
    fn reads_an_8khz_wav_and_measures_it() {
        let dir = testing::temp_dir("audio");
        let call = testing::call(&dir, "sys1", 1); // 3 s of silence at 8 kHz
        let pcm = read_wav(&call.files.wav).unwrap();
        assert_eq!((pcm.rate, pcm.samples.len()), (8000, 24000));
        assert_eq!(rms(&pcm.samples), 0.0);
        assert!((rms(&sine(8000, 1000.0, 1.0, 0.5)) - 0.3536).abs() < 0.01);
    }

    #[test]
    fn reads_a_16khz_stereo_wav_and_downmixes() {
        let tone = sine(16000, 1000.0, 1.0, 0.5);
        let path = testing::temp_dir("audio").join("stereo.wav");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let spec = hound::WavSpec { channels: 2, sample_rate: 16000, bits_per_sample: 16, sample_format: hound::SampleFormat::Int };
        let mut w = hound::WavWriter::create(&path, spec).unwrap();
        for s in &tone {
            w.write_sample((s * 32767.0) as i16).unwrap();
            w.write_sample(0i16).unwrap();
        }
        w.finalize().unwrap();
        let pcm = read_wav(&path).unwrap();
        assert_eq!((pcm.rate, pcm.samples.len()), (16000, 16000));
        assert!((rms(&pcm.samples) - 0.177).abs() < 0.01);
    }

    #[test]
    fn a_missing_wav_error_names_the_file() {
        let err = read_wav(std::path::Path::new("no-such-dir/nope.wav")).unwrap_err();
        assert!(err.contains("nope.wav"), "{err}");
    }

    #[test]
    fn resampling_a_tone_keeps_its_frequency() {
        let out = resample_to_16k(&Pcm { rate: 8000, samples: sine(8000, 1000.0, 1.0, 0.5) });
        assert_eq!(out.len(), 16000);
        let ideal = sine(16000, 1000.0, 1.0, 0.5);
        let err = out[800..15200].iter().zip(&ideal[800..15200]).map(|(a, b)| (a - b).powi(2)).sum::<f32>() / 14400.0;
        assert!(err.sqrt() < 0.02, "rms error {}", err.sqrt());
    }

    #[test]
    fn the_p25_filter_notches_1200_and_keeps_600() {
        let mut keep = sine(16000, 600.0, 1.0, 0.5);
        let mut notch = sine(16000, 1200.0, 1.0, 0.5);
        p25_filter(&mut keep);
        p25_filter(&mut notch);
        assert!(rms(&keep[4000..]) > 0.2, "600 Hz kept");
        assert!(rms(&notch[4000..]) < 0.05, "1200 Hz notched");
        assert!(keep.iter().map(|s| s.abs()).fold(0.0, f32::max) <= 0.71); // -3 dBFS
    }

    #[test]
    fn the_p25_filter_leaves_silence_silent() {
        let mut s = vec![0.0f32; 100];
        p25_filter(&mut s);
        assert!(s.iter().all(|x| *x == 0.0));
    }

    #[test]
    fn wav_out_reads_back() {
        let bytes = wav_16k(&sine(16000, 440.0, 0.5, 0.1));
        assert_eq!(&bytes[..4], b"RIFF");
        assert_eq!(bytes.len(), 44 + 8000 * 2);
        let p = read_wav(&write_temp(&bytes)).unwrap();
        assert_eq!((p.rate, p.samples.len()), (16000, 8000));
        assert_eq!(test_tone().len(), 44 + 16000 * 2);
    }
}
