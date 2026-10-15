//! From one feed line to the rows the extract stages write.
//!
//! These are the only entry points the extract stages need: a line either becomes a row or a
//! [`MalformedRecord`] to quarantine. Every check that could make the document builder fail
//! later (other than the size limit) happens here, through the same parser it uses.

use crate::envelope::parse_envelope;
use crate::error::MalformedRecord;
use crate::roles::{PersonFields, distinct_in_order};
use crate::rows::{OrgRow, PersonRow};
use crate::seq::Seq;

/// Parses an organization feed line into its row. The record only has to be an object: the
/// join never reads inside an organization body.
pub fn parse_org_line(line: &[u8], seq: Seq) -> Result<OrgRow, MalformedRecord> {
    let envelope = parse_envelope(line)?;
    Ok(OrgRow {
        org_id: envelope.id,
        seq,
        body: envelope.record_text().as_bytes().to_vec(),
    })
}

/// Parses a person feed line into its row, validating `roles[].organization_id`.
/// The row's edges are [`PersonRow::edges`].
pub fn parse_person_line(line: &[u8], seq: Seq) -> Result<PersonRow, MalformedRecord> {
    let envelope = parse_envelope(line)?;
    let record = envelope.record_text();
    let fields = PersonFields::parse(record)?;
    Ok(PersonRow {
        person_id: envelope.id,
        seq,
        body: record.as_bytes().to_vec(),
        org_ids: distinct_in_order(fields.role_organization_ids()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::MalformedKind;

    #[test]
    fn org_lines_keep_the_raw_record() {
        let row = parse_org_line(
            br#"{"id":"5","record":{"name":"A","n":1.10,"x":{"deep":[1]}}}"#,
            Seq::new(1, 2),
        )
        .unwrap();
        assert_eq!(row.org_id, 5);
        assert_eq!(row.seq, Seq::new(1, 2));
        assert_eq!(row.body, br#"{"name":"A","n":1.10,"x":{"deep":[1]}}"#);
    }

    #[test]
    fn person_lines_collect_distinct_org_ids() {
        let line = br#"{"id":8,"record":{"roles":[{"organization_id":4},{"organization_id":null},{"organization_id":"4"},{"organization_id":2}]}}"#;
        let row = parse_person_line(line, Seq::new(0, 1)).unwrap();
        assert_eq!(row.person_id, 8);
        assert_eq!(row.org_ids, [4, 2]);
        assert_eq!(row.edges().len(), 2);
    }

    #[test]
    fn person_lines_reject_bad_roles() {
        let err =
            parse_person_line(br#"{"id":8,"record":{"roles":[3]}}"#, Seq::default()).unwrap_err();
        assert_eq!(err.kind(), MalformedKind::Record);
    }

    #[test]
    fn org_lines_do_not_read_roles() {
        // An organization with a field named roles is still just an object.
        assert!(parse_org_line(br#"{"id":1,"record":{"roles":7}}"#, Seq::default()).is_ok());
    }
}
