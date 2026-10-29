//! The transcript store: one SQLite file with the transcripts, their segments
//! and a full-text index over the text.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use rusqlite::types::Value;
use rusqlite::{Connection, Row as SqlRow, params, params_from_iter};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Schema and opening
// ---------------------------------------------------------------------------

/// The schema version this build writes and the newest it can read.
const VERSION: i64 = 1;

pub const SCHEMA: &str = "
CREATE TABLE transcripts (
  path TEXT PRIMARY KEY, short_name TEXT NOT NULL, talkgroup INTEGER NOT NULL,
  start_ms INTEGER NOT NULL, length_ms INTEGER NOT NULL,
  status TEXT NOT NULL,
  reason TEXT, text TEXT, language TEXT,
  engine TEXT NOT NULL, model_asked TEXT NOT NULL, model_reported TEXT,
  prompt TEXT, took_ms INTEGER, attempts INTEGER NOT NULL DEFAULT 1,
  transcribed_ms INTEGER NOT NULL, raw TEXT
);
CREATE TABLE segments (
  path TEXT NOT NULL REFERENCES transcripts(path) ON DELETE CASCADE,
  seq INTEGER NOT NULL, unit INTEGER, start_s REAL NOT NULL, end_s REAL NOT NULL, text TEXT NOT NULL,
  PRIMARY KEY (path, seq)
) WITHOUT ROWID;
CREATE INDEX transcripts_start ON transcripts(start_ms);
CREATE INDEX transcripts_tg_start ON transcripts(short_name, talkgroup, start_ms);
CREATE INDEX transcripts_transcribed ON transcripts(transcribed_ms);
CREATE VIRTUAL TABLE transcripts_fts USING fts5(text, content='transcripts', content_rowid='rowid');
CREATE TRIGGER transcripts_ai AFTER INSERT ON transcripts BEGIN
  INSERT INTO transcripts_fts(rowid, text) VALUES (new.rowid, new.text);
END;
CREATE TRIGGER transcripts_ad AFTER DELETE ON transcripts BEGIN
  INSERT INTO transcripts_fts(transcripts_fts, rowid, text) VALUES ('delete', old.rowid, old.text);
END;
CREATE TRIGGER transcripts_au AFTER UPDATE OF text ON transcripts BEGIN
  INSERT INTO transcripts_fts(transcripts_fts, rowid, text) VALUES ('delete', old.rowid, old.text);
  INSERT INTO transcripts_fts(rowid, text) VALUES (new.rowid, new.text);
END;
";

fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

/// Why a database would not open.
enum OpenFail {
    /// The file is not a database, or is damaged: safe to move aside.
    Corrupt(String),
    /// Anything else (busy, I/O, permissions, full disk): the file is left alone.
    Other(String),
}

fn classify(e: rusqlite::Error, path: &Path) -> OpenFail {
    use rusqlite::ErrorCode::{DatabaseCorrupt, NotADatabase};
    match &e {
        rusqlite::Error::SqliteFailure(f, _) if matches!(f.code, NotADatabase | DatabaseCorrupt) => OpenFail::Corrupt(e.to_string()),
        _ => OpenFail::Other(format!("cannot open the transcript database {}: {e}", path.display())),
    }
}

/// Opens `path` with the pragmas set, and checks it.
fn open_checked(path: &Path) -> Result<Connection, OpenFail> {
    let c = |e| classify(e, path);
    let conn = Connection::open(path).map_err(c)?;
    conn.busy_timeout(std::time::Duration::from_secs(5)).map_err(c)?;
    conn.query_row("PRAGMA journal_mode = WAL", [], |_| Ok(())).map_err(c)?;
    conn.pragma_update(None, "synchronous", "NORMAL").map_err(c)?;
    conn.pragma_update(None, "foreign_keys", true).map_err(c)?;
    let check: String = conn.query_row("PRAGMA quick_check", [], |r| r.get(0)).map_err(c)?;
    if check != "ok" {
        return Err(OpenFail::Corrupt(format!("quick_check: {check}")));
    }
    Ok(conn)
}

/// Moves the database and its journal files aside; returns the main file's new name.
/// Any file that exists but cannot be moved is an `Err`.
fn move_aside(path: &Path) -> Result<String, String> {
    let secs = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    let mut moved = path.display().to_string();
    for suffix in ["", "-wal", "-shm"] {
        let mut from = path.as_os_str().to_owned();
        from.push(suffix);
        let mut to = from.clone();
        to.push(format!(".corrupt-{secs}"));
        if Path::new(&from).exists() {
            std::fs::rename(&from, &to).map_err(|e| format!("cannot move the corrupt transcript database file {} aside: {e}", Path::new(&from).display()))?;
            if suffix.is_empty() {
                moved = Path::new(&to).display().to_string();
            }
        }
    }
    Ok(moved)
}

fn init(conn: &Connection) -> Result<(), String> {
    let version: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0)).map_err(err)?;
    if version > VERSION {
        return Err(format!("the transcript database is from a newer version of this plugin (schema {version}, this build reads {VERSION})"));
    }
    if version == 0 {
        conn.execute_batch(&format!("BEGIN;{SCHEMA}PRAGMA user_version = {VERSION};COMMIT;")).map_err(err)?;
    }
    Ok(())
}

/// The transcript database. Cloning shares the connection.
#[derive(Clone, Debug)]
pub struct Store(pub Arc<Mutex<Connection>>);

impl Store {
    /// Opens (creating if need be) the database at `path`. A file that will
    /// not open or fails `quick_check` is moved aside and a fresh one started;
    /// `warn` is told. A database from a newer schema is an `Err`.
    pub fn open(path: &Path, warn: &dyn Fn(String)) -> Result<Store, String> {
        let conn = match open_checked(path) {
            Ok(c) => c,
            Err(OpenFail::Other(why)) => return Err(why),
            Err(OpenFail::Corrupt(why)) => {
                let moved = move_aside(path)?;
                warn(format!("the transcript database was corrupt ({why}); moved it to {moved} and started a new one"));
                match open_checked(path) {
                    Ok(c) => c,
                    Err(OpenFail::Corrupt(why) | OpenFail::Other(why)) => return Err(why),
                }
            }
        };
        init(&conn)?;
        Ok(Store(Arc::new(Mutex::new(conn))))
    }

    /// A throwaway database, for tests.
    pub fn in_memory() -> Store {
        let conn = Connection::open_in_memory().expect("in-memory database");
        conn.pragma_update(None, "foreign_keys", true).expect("foreign_keys");
        init(&conn).expect("schema");
        Store(Arc::new(Mutex::new(conn)))
    }

    fn conn(&self) -> Result<MutexGuard<'_, Connection>, String> {
        self.0.lock().map_err(|_| "the transcript store's lock is poisoned".to_string())
    }
}

// ---------------------------------------------------------------------------
// Rows and conversions
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Status {
    Ok,
    #[default]
    Skipped,
    Empty,
    Rejected,
    Failed,
}

impl Status {
    pub fn as_str(self) -> &'static str {
        match self {
            Status::Ok => "ok",
            Status::Skipped => "skipped",
            Status::Empty => "empty",
            Status::Rejected => "rejected",
            Status::Failed => "failed",
        }
    }

    pub fn parse(s: &str) -> Option<Status> {
        Some(match s {
            "ok" => Status::Ok,
            "skipped" => Status::Skipped,
            "empty" => Status::Empty,
            "rejected" => Status::Rejected,
            "failed" => Status::Failed,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Row {
    pub path: String,
    pub short_name: String,
    pub talkgroup: u32,
    pub start_ms: i64,
    pub length_ms: i64,
    pub status: Status,
    pub reason: Option<String>,
    pub text: Option<String>,
    pub language: Option<String>,
    pub engine: String,
    pub model_asked: String,
    pub model_reported: Option<String>,
    pub prompt: Option<String>,
    pub took_ms: Option<u64>,
    pub attempts: u32,
    pub transcribed_ms: i64,
    pub raw: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SegmentRow {
    pub seq: u32,
    pub unit: Option<i64>,
    pub start_s: f64,
    pub end_s: f64,
    pub text: String,
}

/// The columns of `transcripts`, in the order `read_row` takes them, qualified by alias `t`.
const COLUMNS: &str = "t.path, t.short_name, t.talkgroup, t.start_ms, t.length_ms, t.status, t.reason, t.text, t.language, \
     t.engine, t.model_asked, t.model_reported, t.prompt, t.took_ms, t.attempts, t.transcribed_ms, t.raw";

fn read_row(r: &SqlRow) -> rusqlite::Result<Row> {
    let status: String = r.get(5)?;
    Ok(Row {
        path: r.get(0)?,
        short_name: r.get(1)?,
        talkgroup: r.get::<_, i64>(2)? as u32,
        start_ms: r.get(3)?,
        length_ms: r.get(4)?,
        status: Status::parse(&status).ok_or_else(|| rusqlite::Error::InvalidColumnType(5, format!("status {status:?}"), rusqlite::types::Type::Text))?,
        reason: r.get(6)?,
        text: r.get(7)?,
        language: r.get(8)?,
        engine: r.get(9)?,
        model_asked: r.get(10)?,
        model_reported: r.get(11)?,
        prompt: r.get(12)?,
        took_ms: r.get::<_, Option<i64>>(13)?.map(|v| v as u64),
        attempts: r.get::<_, i64>(14)? as u32,
        transcribed_ms: r.get(15)?,
        raw: r.get(16)?,
    })
}

fn segments_of(conn: &Connection, path: &str) -> Result<Vec<SegmentRow>, String> {
    let mut st = conn.prepare_cached("SELECT seq, unit, start_s, end_s, text FROM segments WHERE path = ? ORDER BY seq").map_err(err)?;
    st.query_map([path], |r| Ok(SegmentRow { seq: r.get::<_, i64>(0)? as u32, unit: r.get(1)?, start_s: r.get(2)?, end_s: r.get(3)?, text: r.get(4)? }))
        .map_err(err)?
        .collect::<Result<_, _>>()
        .map_err(err)
}

fn with_segments(conn: &Connection, rows: Vec<Row>) -> Result<Vec<(Row, Vec<SegmentRow>)>, String> {
    rows.into_iter()
        .map(|r| {
            let segs = segments_of(conn, &r.path)?;
            Ok((r, segs))
        })
        .collect()
}

/// `?,?,?` for `n` parameters.
fn placeholders(n: usize) -> String {
    vec!["?"; n].join(",")
}

// ---------------------------------------------------------------------------
// The JSON shape
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Transcript {
    pub path: String,
    pub system: String,
    pub talkgroup: u32,
    pub start_ms: i64,
    pub length_ms: i64,
    pub status: String,
    pub reason: Option<String>,
    pub text: Option<String>,
    pub language: Option<String>,
    pub model: String,
    pub transcribed_ms: i64,
    pub segments: Vec<TranscriptSegment>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TranscriptSegment {
    pub unit: Option<i64>,
    pub start: f64,
    pub end: f64,
    pub text: String,
}

impl Transcript {
    pub fn from_row(row: &Row, segments: &[SegmentRow]) -> Transcript {
        Transcript {
            path: row.path.clone(),
            system: row.short_name.clone(),
            talkgroup: row.talkgroup,
            start_ms: row.start_ms,
            length_ms: row.length_ms,
            status: row.status.as_str().to_string(),
            reason: row.reason.clone(),
            text: row.text.clone(),
            language: row.language.clone(),
            model: row.model_reported.clone().unwrap_or_else(|| row.model_asked.clone()),
            transcribed_ms: row.transcribed_ms,
            segments: segments.iter().map(|s| TranscriptSegment { unit: s.unit, start: s.start_s, end: s.end_s, text: s.text.clone() }).collect(),
        }
    }
}

// ---------------------------------------------------------------------------
// Writes and point reads
// ---------------------------------------------------------------------------

impl Store {
    /// Stores `row` and its `segments`, replacing whatever the path had, in one transaction.
    pub fn put(&self, row: &Row, segments: &[SegmentRow]) -> Result<(), String> {
        let mut conn = self.conn()?;
        let tx = conn.transaction().map_err(err)?;
        tx.execute("DELETE FROM transcripts WHERE path = ?", [&row.path]).map_err(err)?;
        tx.execute(
            "INSERT INTO transcripts (path, short_name, talkgroup, start_ms, length_ms, status, reason, text, language, \
             engine, model_asked, model_reported, prompt, took_ms, attempts, transcribed_ms, raw) \
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            params![
                row.path,
                row.short_name,
                row.talkgroup,
                row.start_ms,
                row.length_ms,
                row.status.as_str(),
                row.reason,
                row.text,
                row.language,
                row.engine,
                row.model_asked,
                row.model_reported,
                row.prompt,
                row.took_ms.map(|v| v as i64),
                row.attempts,
                row.transcribed_ms,
                row.raw,
            ],
        )
        .map_err(err)?;
        {
            let mut st = tx.prepare_cached("INSERT INTO segments (path, seq, unit, start_s, end_s, text) VALUES (?,?,?,?,?,?)").map_err(err)?;
            for s in segments {
                st.execute(params![row.path, s.seq, s.unit, s.start_s, s.end_s, s.text]).map_err(err)?;
            }
        }
        tx.commit().map_err(err)
    }

    pub fn get(&self, path: &str) -> Result<Option<(Row, Vec<SegmentRow>)>, String> {
        let conn = self.conn()?;
        let row = {
            let mut st = conn.prepare_cached(&format!("SELECT {COLUMNS} FROM transcripts t WHERE t.path = ?")).map_err(err)?;
            let mut rows = st.query_map([path], read_row).map_err(err)?;
            rows.next().transpose().map_err(err)?
        };
        match row {
            Some(r) => {
                let segs = segments_of(&conn, &r.path)?;
                Ok(Some((r, segs)))
            }
            None => Ok(None),
        }
    }

    /// The rows for `paths`, in the order given; absent ones are left out.
    pub fn get_many(&self, paths: &[String]) -> Result<Vec<(Row, Vec<SegmentRow>)>, String> {
        if paths.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.conn()?;
        let sql = format!("SELECT {COLUMNS} FROM transcripts t WHERE t.path IN ({})", placeholders(paths.len()));
        let mut found: HashMap<String, Row> = {
            let mut st = conn.prepare(&sql).map_err(err)?;
            let rows = st.query_map(params_from_iter(paths), read_row).map_err(err)?;
            rows.map(|r| r.map(|r| (r.path.clone(), r))).collect::<Result<_, _>>().map_err(err)?
        };
        let ordered: Vec<Row> = paths.iter().filter_map(|p| found.remove(p)).collect();
        with_segments(&conn, ordered)
    }

    /// Status and attempts for those of `paths` that have rows.
    pub fn settled(&self, paths: &[String]) -> Result<HashMap<String, (Status, u32)>, String> {
        if paths.is_empty() {
            return Ok(HashMap::new());
        }
        let conn = self.conn()?;
        let sql = format!("SELECT path, status, attempts FROM transcripts WHERE path IN ({})", placeholders(paths.len()));
        let mut st = conn.prepare(&sql).map_err(err)?;
        let rows = st.query_map(params_from_iter(paths), |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, i64>(2)?))).map_err(err)?;
        let mut out = HashMap::new();
        for r in rows {
            let (path, status, attempts) = r.map_err(err)?;
            let status = Status::parse(&status).ok_or_else(|| format!("unknown status {status:?}"))?;
            out.insert(path, (status, attempts as u32));
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Listing
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Query {
    pub system: Option<String>,
    pub talkgroup: Option<u32>,
    pub from: Option<i64>,
    pub to: Option<i64>,
    pub before: Option<i64>,
    pub limit: usize,
}

impl Default for Query {
    fn default() -> Self {
        Query { system: None, talkgroup: None, from: None, to: None, before: None, limit: 100 }
    }
}

#[derive(Debug, Clone)]
pub struct Page {
    pub rows: Vec<(Row, Vec<SegmentRow>)>,
    pub more: bool,
    /// The oldest row's `start_ms` on this page (0 when empty): pass it as `before` for the next.
    pub before: i64,
}

impl Query {
    /// `AND ...` conditions on alias `t`, and their parameters.
    fn conditions(&self) -> (String, Vec<Value>) {
        let mut sql = String::new();
        let mut args = Vec::new();
        if let Some(s) = &self.system {
            sql.push_str(" AND t.short_name = ?");
            args.push(Value::Text(s.clone()));
        }
        if let Some(tg) = self.talkgroup {
            sql.push_str(" AND t.talkgroup = ?");
            args.push(Value::Integer(tg.into()));
        }
        if let Some(v) = self.from {
            sql.push_str(" AND t.start_ms >= ?");
            args.push(Value::Integer(v));
        }
        if let Some(v) = self.to {
            sql.push_str(" AND t.start_ms < ?");
            args.push(Value::Integer(v));
        }
        if let Some(v) = self.before {
            sql.push_str(" AND t.start_ms < ?");
            args.push(Value::Integer(v));
        }
        (sql, args)
    }
}

/// Runs `SELECT {select} {from_where}` newest first and returns the top `limit` rows
/// plus every further row sharing the last one's `start_ms`, and whether anything
/// older exists. `from_where` ends in conditions on alias `t`; `args` are its parameters.
fn fetch_whole_ties<X>(
    conn: &Connection,
    select: &str,
    from_where: &str,
    mut args: Vec<Value>,
    limit: usize,
    read: impl Fn(&SqlRow) -> rusqlite::Result<(Row, X)>,
) -> Result<(Vec<(Row, X)>, bool), String> {
    let run = |sql: &str, args: Vec<Value>| -> Result<Vec<(Row, X)>, String> {
        let mut st = conn.prepare(sql).map_err(err)?;
        st.query_map(params_from_iter(args), &read).map_err(err)?.collect::<Result<_, _>>().map_err(err)
    };
    let mut with_limit = args.clone();
    with_limit.push(Value::Integer(limit as i64 + 1));
    let mut rows = run(&format!("SELECT {select} {from_where} ORDER BY t.start_ms DESC, t.path DESC LIMIT ?"), with_limit)?;
    if rows.len() <= limit {
        return Ok((rows, false));
    }
    if limit == 0 {
        return Ok((Vec::new(), true));
    }
    let edge = rows[limit - 1].0.start_ms;
    if rows[limit].0.start_ms != edge {
        rows.truncate(limit);
        return Ok((rows, true));
    }
    // The tie runs past the limit: take all of it.
    rows.retain(|(r, _)| r.start_ms != edge);
    let mut tied = args.clone();
    tied.push(Value::Integer(edge));
    rows.extend(run(&format!("SELECT {select} {from_where} AND t.start_ms = ? ORDER BY t.path DESC"), tied)?);
    args.push(Value::Integer(edge));
    let older = conn.prepare(&format!("SELECT 1 {from_where} AND t.start_ms < ? LIMIT 1")).and_then(|mut st| st.exists(params_from_iter(args))).map_err(err)?;
    Ok((rows, older))
}

impl Store {
    /// Newest first. A page never splits a run of rows that share a `start_ms`
    /// (the cursor is only a time), so it may hold more than `limit` rows.
    pub fn page(&self, q: &Query) -> Result<Page, String> {
        let (cond, args) = q.conditions();
        let from_where = format!("FROM transcripts t WHERE 1=1{cond}");
        let conn = self.conn()?;
        let (rows, more) = fetch_whole_ties(&conn, COLUMNS, &from_where, args, q.limit, |r| Ok((read_row(r)?, ())))?;
        let before = rows.last().map_or(0, |(r, _)| r.start_ms);
        let rows = with_segments(&conn, rows.into_iter().map(|(r, _)| r).collect())?;
        Ok(Page { rows, more, before })
    }

    /// Full-text search over the transcripts that have text; each hit carries
    /// its snippet. Returns `(hits, more, before)` like a page, with the same tie rule.
    #[allow(clippy::type_complexity)]
    pub fn search(&self, text: &str, q: &Query) -> Result<(Vec<(Row, Vec<SegmentRow>, String)>, bool, i64), String> {
        let m = fts_query(text);
        if m.is_empty() {
            return Ok((Vec::new(), false, 0));
        }
        let (cond, rest) = q.conditions();
        let mut args = vec![Value::Text(m)];
        args.extend(rest);
        let select = format!("{COLUMNS}, snippet(transcripts_fts, 0, '<b>', '</b>', '…', 12)");
        let from_where = format!(
            "FROM transcripts_fts JOIN transcripts t ON t.rowid = transcripts_fts.rowid \
             WHERE transcripts_fts MATCH ? AND t.status = 'ok'{cond}"
        );
        let conn = self.conn()?;
        let (found, more) = fetch_whole_ties(&conn, &select, &from_where, args, q.limit, |r| Ok((read_row(r)?, r.get::<_, String>(17)?)))?;
        let before = found.last().map_or(0, |(r, _)| r.start_ms);
        let hits = found
            .into_iter()
            .map(|(r, snippet)| {
                let segs = segments_of(&conn, &r.path)?;
                Ok((r, segs, snippet))
            })
            .collect::<Result<_, String>>()?;
        Ok((hits, more, before))
    }

    /// Rows transcribed after `transcribed_ms`, oldest first, for replaying to a late subscriber.
    pub fn since(&self, transcribed_ms: i64, limit: usize) -> Result<Vec<(Row, Vec<SegmentRow>)>, String> {
        let conn = self.conn()?;
        let rows: Vec<Row> = {
            let mut st = conn
                .prepare(&format!("SELECT {COLUMNS} FROM transcripts t WHERE t.transcribed_ms > ? ORDER BY t.transcribed_ms, t.path LIMIT ?"))
                .map_err(err)?;
            st.query_map(params![transcribed_ms, limit as i64], read_row).map_err(err)?.collect::<Result<_, _>>().map_err(err)?
        };
        with_segments(&conn, rows)
    }

    /// Paths whose text came from a model other than `model` (the one the
    /// engine named, else the one asked for), oldest first; failed and
    /// skipped calls have nothing to redo.
    pub fn retranscribe_candidates(&self, model: &str, since_ms: Option<i64>) -> Result<Vec<String>, String> {
        let conn = self.conn()?;
        let mut st = conn
            .prepare(
                "SELECT path FROM transcripts WHERE status IN ('ok','empty','rejected') \
                 AND COALESCE(model_reported, model_asked) <> ?1 AND (?2 IS NULL OR start_ms >= ?2) \
                 ORDER BY start_ms, path",
            )
            .map_err(err)?;
        st.query_map(params![model, since_ms], |r| r.get(0)).map_err(err)?.collect::<Result<_, _>>().map_err(err)
    }
}

/// Turns user input into an FTS5 query that matches all its words: each
/// whitespace-separated token quoted, so no operator or syntax gets through.
pub fn fts_query(q: &str) -> String {
    q.split_whitespace().map(|t| format!("\"{}\"", t.replace('"', "\"\""))).collect::<Vec<_>>().join(" ")
}

// ---------------------------------------------------------------------------
// Maintenance
// ---------------------------------------------------------------------------

impl Store {
    /// Deletes transcripts older than `days` days before `now_ms` (0 days keeps everything);
    /// returns how many went. The lock is taken per batch of 500, so the pipeline
    /// and the HTTP side are not held up for the whole sweep.
    pub fn retain(&self, days: u32, now_ms: i64) -> Result<usize, String> {
        if days == 0 {
            return Ok(0);
        }
        let cutoff = now_ms - i64::from(days) * 86_400_000;
        let mut total = 0;
        loop {
            let n = self
                .conn()?
                .execute("DELETE FROM transcripts WHERE path IN (SELECT path FROM transcripts WHERE start_ms < ? LIMIT 500)", [cutoff])
                .map_err(err)?;
            total += n;
            if n < 500 {
                return Ok(total);
            }
        }
    }

    /// How many `ok` transcripts were made at or after `ms`.
    pub fn transcribed_since(&self, ms: i64) -> Result<u64, String> {
        let conn = self.conn()?;
        let n: i64 = conn.query_row("SELECT COUNT(*) FROM transcripts WHERE status = 'ok' AND transcribed_ms >= ?", [ms], |r| r.get(0)).map_err(err)?;
        Ok(n as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use trunk_recorder_plugin::testing;

    fn row(path: &str, start_ms: i64, status: Status, text: Option<&str>) -> Row {
        Row {
            path: path.into(),
            short_name: "sys1".into(),
            talkgroup: 101,
            start_ms,
            length_ms: 4000,
            status,
            text: text.map(String::from),
            engine: "e".into(),
            model_asked: "default".into(),
            model_reported: Some("m1".into()),
            attempts: 1,
            transcribed_ms: start_ms + 10,
            ..Default::default()
        }
    }

    #[test]
    fn put_get_and_replace_keep_segments_in_step() {
        let s = Store::in_memory();
        s.put(
            &row("a", 1000, Status::Ok, Some("tower seventy three")),
            &[SegmentRow { seq: 0, unit: Some(1001), start_s: 0.2, end_s: 1.3, text: "tower seventy three".into() }],
        )
        .unwrap();
        let (r, segs) = s.get("a").unwrap().unwrap();
        assert_eq!((r.status, segs.len(), segs[0].unit), (Status::Ok, 1, Some(1001)));
        s.put(&row("a", 1000, Status::Empty, None), &[]).unwrap();
        let (r, segs) = s.get("a").unwrap().unwrap();
        assert_eq!((r.status, segs.len()), (Status::Empty, 0));
        assert!(s.get("b").unwrap().is_none());
        let t = Transcript::from_row(&row("a", 1000, Status::Ok, Some("x")), &[]);
        assert_eq!(
            serde_json::to_value(&t).unwrap(),
            json!({ "path": "a", "system": "sys1", "talkgroup": 101, "startMs": 1000, "lengthMs": 4000, "status": "ok", "reason": null, "text": "x", "language": null, "model": "m1", "transcribedMs": 1010, "segments": [] })
        );
    }

    #[test]
    fn pages_newest_first_with_a_cursor() {
        let s = Store::in_memory();
        for i in 0..7 {
            s.put(&row(&format!("p{i}"), 1000 * i, Status::Ok, Some("t")), &[]).unwrap();
        }
        let p = s.page(&Query { limit: 3, ..Default::default() }).unwrap();
        assert_eq!((p.rows.iter().map(|(r, _)| r.path.as_str()).collect::<Vec<_>>(), p.more, p.before), (vec!["p6", "p5", "p4"], true, 4000));
        let p = s.page(&Query { limit: 3, before: Some(4000), ..Default::default() }).unwrap();
        assert_eq!(p.rows[0].0.path, "p3");
        let p = s.page(&Query { limit: 10, from: Some(2000), to: Some(5000), ..Default::default() }).unwrap();
        assert_eq!((p.rows.len(), p.more), (3, false)); // 2000, 3000, 4000
        let p = s.page(&Query { limit: 10, talkgroup: Some(5), ..Default::default() }).unwrap();
        assert!(p.rows.is_empty());
    }

    #[test]
    fn search_finds_text_with_a_snippet_and_survives_fts_syntax() {
        let s = Store::in_memory();
        s.put(&row("a", 1000, Status::Ok, Some("Engine 3 responding to the structure fire")), &[]).unwrap();
        s.put(&row("b", 2000, Status::Rejected, Some("structure structure structure")), &[]).unwrap();
        let (hits, more, _) = s.search("structure", &Query { limit: 10, ..Default::default() }).unwrap();
        assert_eq!((hits.len(), more), (1, false));
        assert!(hits[0].2.contains("<b>structure</b>"), "{}", hits[0].2);
        for q in ["\"", "AND", "fire*", "(", "a \"b", "NOT fire"] {
            assert!(s.search(q, &Query { limit: 10, ..Default::default() }).is_ok(), "{q}");
        }
        assert_eq!(fts_query("a \"b\" c"), r#""a" "\"b\"" "c""#.replace("\\\"", "\"\""));
        assert_eq!(fts_query("  "), "");
    }

    #[test]
    fn settled_candidates_since_and_retention() {
        let s = Store::in_memory();
        s.put(&row("a", 1000, Status::Ok, Some("t")), &[]).unwrap();
        s.put(&Row { model_reported: Some("m2".into()), ..row("b", 2000, Status::Empty, None) }, &[]).unwrap();
        s.put(&row("c", 3000, Status::Skipped, None), &[]).unwrap();
        s.put(&Row { attempts: 3, ..row("d", 4000, Status::Failed, None) }, &[]).unwrap();
        let got = s.settled(&["a".into(), "d".into(), "zz".into()]).unwrap();
        assert_eq!((got["a"], got["d"], got.get("zz")), ((Status::Ok, 1), (Status::Failed, 3), None));
        assert_eq!(s.retranscribe_candidates("m2", None).unwrap(), ["a"]);
        assert_eq!(s.retranscribe_candidates("m3", Some(1500)).unwrap(), ["b"]);
        assert_eq!(s.since(1010, 10).unwrap().iter().map(|(r, _)| r.path.as_str()).collect::<Vec<_>>(), ["b", "c", "d"]);
        assert_eq!(s.transcribed_since(0).unwrap(), 1);
        assert_eq!(s.retain(0, 1_000_000).unwrap(), 0);
        assert_eq!(s.retain(1, 2500 + 86_400_000).unwrap(), 2);
        assert!(s.get("a").unwrap().is_none() && s.get("c").unwrap().is_some());

        // No reported model: the one asked for is the transcript's model.
        s.put(&Row { model_reported: None, model_asked: "big".into(), ..row("e", 5000, Status::Ok, Some("t")) }, &[]).unwrap();
        assert!(s.retranscribe_candidates("big", None).unwrap().is_empty());
        assert_eq!(s.retranscribe_candidates("m2", None).unwrap(), ["e"]);
    }

    #[test]
    fn transcribed_ms_is_indexed() {
        let s = Store::in_memory();
        let conn = s.0.lock().unwrap();
        let plan: String =
            conn.query_row("EXPLAIN QUERY PLAN SELECT COUNT(*) FROM transcripts WHERE status = 'ok' AND transcribed_ms >= 0", [], |r| r.get(3)).unwrap();
        assert!(plan.contains("transcripts_transcribed"), "{plan}");
    }

    #[test]
    fn a_corrupt_file_is_moved_aside_and_a_newer_version_refused() {
        let dir = testing::temp_dir("store");
        let p = dir.join("transcripts.sqlite");
        std::fs::write(&p, b"this is not a database").unwrap();
        let warned = std::cell::RefCell::new(Vec::new());
        let s = Store::open(&p, &|m| warned.borrow_mut().push(m)).unwrap();
        assert!(warned.borrow()[0].contains("corrupt"), "{:?}", warned.borrow());
        assert!(std::fs::read_dir(&dir).unwrap().flatten().any(|e| e.file_name().to_string_lossy().contains(".corrupt-")));
        s.put(&row("a", 1, Status::Ok, Some("t")), &[]).unwrap();
        drop(s);
        let s = Store::open(&p, &|_| {}).unwrap();
        assert!(s.get("a").unwrap().is_some(), "reopening keeps data");
        s.0.lock().unwrap().pragma_update(None, "user_version", 2).unwrap();
        drop(s);
        assert!(Store::open(&p, &|_| {}).unwrap_err().contains("newer"));
    }

    #[test]
    fn a_page_never_splits_rows_that_share_a_start() {
        let s = Store::in_memory();
        for (p, t) in [("n0", 5000), ("n1", 4000), ("t0", 3000), ("t1", 3000), ("t2", 3000), ("t3", 3000), ("o0", 1000)] {
            s.put(&row(p, t, Status::Ok, Some("tie")), &[]).unwrap();
        }
        let all = ["n0", "n1", "t3", "t2", "t1", "t0", "o0"];
        let first = s.page(&Query { limit: 3, ..Default::default() }).unwrap();
        assert_eq!((first.rows.len(), first.more, first.before), (6, true, 3000));
        for limit in 1..=8 {
            let (mut got, mut before) = (Vec::new(), None);
            loop {
                let p = s.page(&Query { limit, before, ..Default::default() }).unwrap();
                got.extend(p.rows.into_iter().map(|h| h.0.path));
                if !p.more {
                    break;
                }
                before = Some(p.before);
            }
            assert_eq!(got, all, "page, limit {limit}");
            let (mut got, mut before) = (Vec::new(), None);
            loop {
                let (hits, more, b) = s.search("tie", &Query { limit, before, ..Default::default() }).unwrap();
                got.extend(hits.into_iter().map(|h| h.0.path));
                if !more {
                    break;
                }
                before = Some(b);
            }
            assert_eq!(got, all, "search, limit {limit}");
        }
    }

    #[test]
    fn the_full_text_index_follows_a_replace() {
        let s = Store::in_memory();
        s.put(&row("a", 1000, Status::Ok, Some("alpha")), &[]).unwrap();
        s.put(&row("a", 1000, Status::Ok, Some("beta")), &[]).unwrap();
        let q = Query { limit: 10, ..Default::default() };
        assert_eq!(s.search("alpha", &q).unwrap().0.len(), 0);
        assert_eq!(s.search("beta", &q).unwrap().0.len(), 1);
        s.0.lock().unwrap().execute("INSERT INTO transcripts_fts(transcripts_fts) VALUES('integrity-check')", []).unwrap();
    }

    #[test]
    fn a_locked_database_is_an_error_and_left_in_place() {
        let dir = testing::temp_dir("store-locked");
        let p = dir.join("transcripts.sqlite");
        drop(Store::open(&p, &|_| {}).unwrap());
        let holder = rusqlite::Connection::open(&p).unwrap();
        holder.execute_batch("PRAGMA locking_mode = EXCLUSIVE; BEGIN EXCLUSIVE;").unwrap();
        let warned = std::cell::RefCell::new(Vec::new());
        let r = Store::open(&p, &|m| warned.borrow_mut().push(m));
        assert!(r.is_err());
        assert!(warned.borrow().is_empty());
        assert!(!std::fs::read_dir(&dir).unwrap().flatten().any(|e| e.file_name().to_string_lossy().contains(".corrupt-")));
        drop(holder);
        assert!(Store::open(&p, &|_| {}).is_ok());
    }
}
