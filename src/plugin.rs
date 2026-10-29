//! The plugin: starts with validated settings (the rest arrives in later tasks).

use trunk_recorder_plugin::{Host, Manifest, Plugin, Setup, topic};

use crate::config::{Config, SystemConfig};

pub struct Transcribe {
    #[allow(dead_code)] // used by the tasks that follow
    host: Host,
}

impl Plugin for Transcribe {
    type Config = Config;
    type SystemConfig = SystemConfig;

    fn manifest() -> Manifest {
        Manifest {
            name: "Transcribe".into(),
            subscribe: vec![topic::CALL_CONCLUDED.into()],
            audio_formats: vec![],
            live_settings: true,
            ..trunk_recorder_plugin::manifest!()
        }
    }

    fn start(host: Host, setup: Setup<Config, SystemConfig>) -> Result<Self, String> {
        setup.config.validate()?;
        Ok(Transcribe { host })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use trunk_recorder_plugin::{HostMessage, testing};

    #[test]
    fn the_manifest_is_transcribe() {
        let m = trunk_recorder_plugin::describe::<Transcribe>();
        assert_eq!((m.id.as_str(), m.name.as_str(), m.api), ("transcribe", "Transcribe", 1));
        assert_eq!(m.subscribe, ["call.concluded"]);
        assert!(m.live_settings && m.config.is_some() && m.system_config.is_some());
    }

    #[test]
    fn bad_settings_stop_it_and_good_ones_start_it() {
        let dir = testing::temp_dir("transcribe");
        let out = testing::run::<Transcribe>([HostMessage::Hello(testing::hello(&dir, json!({})))]);
        assert_eq!(out.exit_code, trunk_recorder_plugin::EXIT_CONFIG);
        let out = testing::run::<Transcribe>([HostMessage::Hello(testing::hello(&dir, json!({ "engine": { "url": "http://127.0.0.1:1" } })))]);
        assert!(out.ready());
    }
}
