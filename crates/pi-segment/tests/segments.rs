//! Round trips, rolling, and every way a segment can be damaged.

use std::io::{self, Read};

use pi_model::{DocRow, JoinedRow, OrgRow, Seq};
use pi_segment::{
    FileSink, MAGIC, MemorySink, SegmentConfig, SegmentEncoder, SegmentError, SegmentReader,
    SegmentWriter, decode_segment, encode_segment,
};
use proptest::prelude::*;

/// Bytes that zstd cannot compress, so segment sizes are predictable.
fn noise(seed: u64, len: usize) -> Vec<u8> {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x.to_le_bytes()[0]
        })
        .collect()
}

fn org(i: u64, len: usize) -> OrgRow {
    OrgRow {
        org_id: i,
        seq: Seq::new(0, i),
        body: noise(i, len),
    }
}

fn read_all<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<Vec<T>, SegmentError> {
    decode_segment(bytes)
}

#[test]
fn rows_round_trip() {
    let rows: Vec<OrgRow> = (0..1000).map(|i| org(i, (i % 50) as usize)).collect();
    let bytes = encode_segment(&rows).unwrap();
    assert_eq!(&bytes[..8], MAGIC);
    assert_eq!(read_all::<OrgRow>(&bytes).unwrap(), rows);

    let joined = vec![
        JoinedRow {
            person_id: 1,
            org_id: 2,
            seq: Seq::new(1, 1),
            body: None,
        },
        JoinedRow {
            person_id: 1,
            org_id: 3,
            seq: Seq::new(1, 1),
            body: Some(b"{\"a\":1}".to_vec()),
        },
    ];
    assert_eq!(
        read_all::<JoinedRow>(&encode_segment(&joined).unwrap()).unwrap(),
        joined
    );
}

#[test]
fn empty_segment_has_no_rows() {
    let encoder = SegmentEncoder::<DocRow, _>::new(Vec::new()).unwrap();
    let (bytes, stats) = encoder.finish().unwrap();
    assert_eq!(stats.records, 0);
    assert_eq!(stats.bytes, bytes.len() as u64);
    assert!(read_all::<DocRow>(&bytes).unwrap().is_empty());
}

#[test]
fn rolling_writer_rolls_past_the_target() {
    let config = SegmentConfig {
        target_bytes: 16 << 10,
        ..SegmentConfig::default()
    };
    let mut writer = SegmentWriter::new(MemorySink::new(), config);
    let rows: Vec<OrgRow> = (0..400).map(|i| org(i, 600)).collect();
    let mut segments = Vec::new();
    for row in &rows {
        segments.extend(writer.push(row).unwrap());
    }
    assert_eq!(writer.records(), 400);
    let (last, totals, _sink) = writer.finish().unwrap();
    segments.extend(last);

    assert!(segments.len() >= 10, "{} segments", segments.len());
    assert_eq!(totals.segments as usize, segments.len());
    assert_eq!(totals.records, 400);
    let mut back = Vec::new();
    for (i, segment) in segments.iter().enumerate() {
        assert_eq!(segment.index as usize, i);
        assert_eq!(segment.stats.bytes, segment.bytes.len() as u64);
        if i + 1 < segments.len() {
            assert!(
                segment.stats.bytes >= config.target_bytes,
                "segment {i} closed early"
            );
            // At most about one row (600 noise bytes plus framing) past the target.
            assert!(
                segment.stats.bytes < config.target_bytes + 1024,
                "segment {i} is {} bytes",
                segment.stats.bytes
            );
        }
        back.extend(read_all::<OrgRow>(&segment.bytes).unwrap());
    }
    assert_eq!(back, rows, "rows keep their order across segments");
    assert_eq!(
        totals.bytes,
        segments.iter().map(|s| s.stats.bytes).sum::<u64>()
    );
}

#[test]
fn compressible_rows_roll_close_to_the_target() {
    let config = SegmentConfig {
        target_bytes: 32 << 10,
        ..SegmentConfig::default()
    };
    let mut writer = SegmentWriter::new(MemorySink::new(), config);
    let mut segments = Vec::new();
    for i in 0..20_000u64 {
        let row = DocRow {
            person_id: i,
            body: format!("{{\"id\":{i},\"name\":\"person {}\"}}", i % 97).into_bytes(),
        };
        segments.extend(writer.push(&row).unwrap());
    }
    let (last, totals, _) = writer.finish().unwrap();
    segments.extend(last);
    assert!(segments.len() >= 2, "{} segments", segments.len());
    for segment in &segments[..segments.len() - 1] {
        assert!(segment.stats.bytes >= config.target_bytes);
        assert!(
            segment.stats.bytes < config.target_bytes + 1024,
            "{}",
            segment.stats.bytes
        );
        assert!(
            segment.stats.raw_bytes > segment.stats.bytes,
            "the rows compressed"
        );
    }
    let back: usize = segments
        .iter()
        .map(|s| read_all::<DocRow>(&s.bytes).unwrap().len())
        .sum();
    assert_eq!(back as u64, totals.records);
}

#[test]
fn rolling_writer_without_rows_produces_nothing() {
    let writer = SegmentWriter::<OrgRow, _>::new(MemorySink::new(), SegmentConfig::default());
    let (last, totals, _) = writer.finish().unwrap();
    assert!(last.is_none());
    assert_eq!(totals.segments, 0);
}

#[test]
fn flush_segment_ends_a_segment_early() {
    let mut writer = SegmentWriter::new(MemorySink::with_capacity(1024), SegmentConfig::default());
    assert!(writer.flush_segment().unwrap().is_none());
    writer.push(&org(1, 10)).unwrap();
    let first = writer.flush_segment().unwrap().unwrap();
    writer.push(&org(2, 10)).unwrap();
    let (second, totals, _) = writer.finish().unwrap();
    let second = second.unwrap();
    assert_eq!((first.index, second.index), (0, 1));
    assert_eq!(totals.records, 2);
    assert_eq!(read_all::<OrgRow>(&second.bytes).unwrap(), [org(2, 10)]);
}

#[test]
fn file_sink_streams_segments_to_disk() {
    let dir = tempfile::tempdir().unwrap();
    let config = SegmentConfig {
        target_bytes: 8 << 10,
        ..SegmentConfig::default()
    };
    let mut writer = SegmentWriter::new(FileSink::new(dir.path(), "task-a1"), config);
    let rows: Vec<OrgRow> = (0..100).map(|i| org(i, 500)).collect();
    let mut files = Vec::new();
    for row in &rows {
        files.extend(writer.push(row).unwrap());
    }
    let (last, _, sink) = writer.finish().unwrap();
    files.extend(last);
    assert!(files.len() > 1);
    assert_eq!(sink.dir(), dir.path());
    let mut back = Vec::new();
    for file in &files {
        assert_eq!(
            file.path.file_name().unwrap().to_str().unwrap(),
            format!("task-a1-{}.seg", file.index)
        );
        assert_eq!(
            std::fs::metadata(&file.path).unwrap().len(),
            file.stats.bytes
        );
        let reader = SegmentReader::<OrgRow, _>::open(&file.path).unwrap();
        back.extend(reader.map(Result::unwrap));
    }
    assert_eq!(back, rows);
}

#[test]
fn bad_magic_is_rejected() {
    let mut bytes = encode_segment(&[org(1, 10)]).unwrap();
    bytes[3] = b'X';
    match SegmentReader::<OrgRow, _>::new(&bytes[..]) {
        Err(SegmentError::BadMagic { found, .. }) => assert_eq!(found, b"PISXG001"),
        other => panic!("{:?}", other.err()),
    }
    let err = SegmentReader::<OrgRow, _>::new(&b"{\"json\": true}"[..])
        .err()
        .unwrap();
    assert!(matches!(err, SegmentError::BadMagic { .. }), "{err}");
    let err = SegmentReader::<OrgRow, _>::new(&b"PI"[..]).err().unwrap();
    assert!(matches!(
        err,
        SegmentError::BadMagic { .. } | SegmentError::Truncated { .. }
    ));
}

#[test]
fn truncation_is_reported_at_every_cut() {
    let rows: Vec<OrgRow> = (0..50).map(|i| org(i, 200)).collect();
    let bytes = encode_segment(&rows).unwrap();
    for cut in 0..bytes.len() {
        let result: Result<Vec<OrgRow>, SegmentError> = read_all(&bytes[..cut]);
        match result {
            Err(SegmentError::Truncated { records, .. }) => assert!(records <= 50),
            other => panic!("cut at {cut} of {}: {other:?}", bytes.len()),
        }
    }
    assert_eq!(read_all::<OrgRow>(&bytes).unwrap().len(), 50);
}

#[test]
fn magic_only_and_empty_files_are_truncated() {
    for bytes in [&b""[..], &MAGIC[..]] {
        let result = read_all::<OrgRow>(bytes);
        assert!(
            matches!(result, Err(SegmentError::Truncated { .. })),
            "{result:?}"
        );
    }
}

#[test]
fn corruption_is_detected() {
    let rows: Vec<OrgRow> = (0..50).map(|i| org(i, 200)).collect();
    let bytes = encode_segment(&rows).unwrap();
    // Flip one bit at a time across the compressed body; every flip must surface as an error,
    // never as different rows.
    for pos in (8..bytes.len()).step_by(37) {
        let mut damaged = bytes.clone();
        damaged[pos] ^= 0x10;
        match read_all::<OrgRow>(&damaged) {
            Err(SegmentError::Corrupt { .. } | SegmentError::Truncated { .. }) => {}
            Ok(back) => assert_eq!(back, rows, "flip at {pos} decoded different rows"),
            Err(other) => panic!("flip at {pos}: {other}"),
        }
    }
}

#[test]
fn trailing_garbage_is_corrupt() {
    let mut bytes = encode_segment(&[org(1, 10)]).unwrap();
    bytes.extend_from_slice(b"garbage after the frame");
    let result = read_all::<OrgRow>(&bytes);
    assert!(
        matches!(result, Err(SegmentError::Corrupt { .. })),
        "{result:?}"
    );
}

#[test]
fn wrong_row_type_is_corrupt() {
    let bytes = encode_segment(&[1u8, 2, 3]).unwrap();
    let result = read_all::<DocRow>(&bytes);
    assert!(
        matches!(result, Err(SegmentError::Corrupt { .. })),
        "{result:?}"
    );
    // A row with bytes left over after decoding is also corrupt.
    let bytes = encode_segment(&[(7u64, 9u64)]).unwrap();
    let result = read_all::<u8>(&bytes);
    assert!(
        matches!(result, Err(SegmentError::Corrupt { .. })),
        "{result:?}"
    );
}

#[test]
fn oversized_lengths_are_corrupt() {
    let bytes = encode_segment(&[vec![0u8; 100]]).unwrap();
    let mut reader = SegmentReader::<Vec<u8>, _>::with_max_record_bytes(&bytes[..], 10).unwrap();
    assert!(matches!(
        reader.next(),
        Some(Err(SegmentError::Corrupt { .. }))
    ));
    assert!(reader.next().is_none(), "the reader stops after an error");
}

/// A reader that fails after `ok` bytes.
struct Failing<'a> {
    data: &'a [u8],
    ok: usize,
}

impl Read for Failing<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.ok == 0 {
            return Err(io::Error::other("disk on fire"));
        }
        let n = buf.len().min(self.ok).min(self.data.len());
        buf[..n].copy_from_slice(&self.data[..n]);
        self.data = &self.data[n..];
        self.ok -= n;
        Ok(n)
    }
}

#[test]
fn source_errors_stay_io_errors() {
    let bytes = encode_segment(&(0..200).map(|i| org(i, 300)).collect::<Vec<_>>()).unwrap();
    let reader = SegmentReader::<OrgRow, _>::new(Failing {
        data: &bytes,
        ok: 2000,
    })
    .unwrap();
    let result: Result<Vec<OrgRow>, SegmentError> = reader.collect();
    match result {
        Err(SegmentError::Io(e)) => assert!(e.to_string().contains("disk on fire"), "{e}"),
        other => panic!("{other:?}"),
    }
    let err = SegmentReader::<OrgRow, _>::new(Failing {
        data: &bytes,
        ok: 0,
    })
    .err()
    .unwrap();
    assert!(matches!(err, SegmentError::Io(_)), "{err}");
}

#[test]
fn reader_counts_records() {
    let bytes = encode_segment(&[1u32, 2, 3]).unwrap();
    let mut reader = SegmentReader::<u32, _>::new(&bytes[..]).unwrap();
    assert_eq!(reader.by_ref().count(), 3);
    assert_eq!(reader.records(), 3);
}

fn assert_send<T: Send + 'static>() {}

#[test]
fn readers_and_writers_can_move_to_blocking_threads() {
    assert_send::<SegmentWriter<OrgRow, MemorySink>>();
    assert_send::<SegmentWriter<OrgRow, FileSink>>();
    assert_send::<SegmentReader<OrgRow, std::fs::File>>();
    assert_send::<SegmentReader<OrgRow, io::Cursor<Vec<u8>>>>();
}

proptest! {
    #[test]
    fn arbitrary_rows_round_trip(rows in prop::collection::vec((any::<u64>(), prop::collection::vec(any::<u8>(), 0..300)), 0..60)) {
        let bytes = encode_segment(&rows).unwrap();
        let back: Vec<(u64, Vec<u8>)> = decode_segment(&bytes).unwrap();
        prop_assert_eq!(back, rows);
    }
}
