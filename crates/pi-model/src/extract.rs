//! From one feed line to the rows the extract stages write.
//!
//! These are the only entry points the extract stages need: a line either becomes a row or a
//! [`MalformedRecord`] to quarantine. Every check that could make the document builder fail
//! later (other than the size limit) happens here, through the same parser it uses, and so does
//! the check of the mapped fields' JSON kinds ([`crate::shape`]) that keeps a single record
//! from failing a bulk request.

use crate::envelope::parse_envelope;
use crate::error::{MalformedKind, MalformedRecord};
use crate::json::write_string;
use crate::raw::RawObject;
use crate::roles::{PersonFields, distinct_in_order};
use crate::rows::{OrgRow, PersonRow};
use crate::seq::Seq;
use crate::shape::{ORGANIZATION_FIELDS, PERSON_FIELDS, ROLE_FIELDS, check_fields};

/// Parses an organization feed line into its row. The join never reads inside an organization
/// body, it splices it into `organizations[]` as it is; so its mapped fields' JSON kinds are
/// checked here, and a body that repeats a key is rewritten from the last occurrences
/// ([`org_body`]).
pub fn parse_org_line(line: &[u8], seq: Seq) -> Result<OrgRow, MalformedRecord> {
    let envelope = parse_envelope(line)?;
    let object = RawObject::parse(envelope.record_text())
        .map_err(|e| MalformedRecord::from_json(MalformedKind::Record, &e))?;
    check_fields(&object, ORGANIZATION_FIELDS, None)?;
    Ok(OrgRow {
        org_id: envelope.id,
        seq,
        body: org_body(envelope.record_text(), &object),
    })
}

/// The body an organization row keeps: `text` as it is, or, when a key repeats, the object
/// rewritten from each key's last occurrence, in input order, as the document builder does for
/// persons and roles. Elasticsearch rejects a document with a duplicate key, and the checked
/// value ([`check_fields`] reads the last occurrence) must be the one indexed.
fn org_body(text: &str, object: &RawObject<'_>) -> Vec<u8> {
    let keep = object.last_occurrences();
    if keep.iter().all(|kept| *kept) {
        return text.as_bytes().to_vec();
    }
    let mut out = Vec::with_capacity(text.len());
    out.push(b'{');
    for ((key, value), _) in object.entries.iter().zip(keep).filter(|(_, kept)| *kept) {
        if out.len() > 1 {
            out.push(b',');
        }
        write_string(&mut out, &key.0);
        out.push(b':');
        out.extend_from_slice(value.get().as_bytes());
    }
    out.push(b'}');
    // The extract budget counts `body.len()`: drop the capacity a long discarded value left.
    out.into_boxed_slice().into_vec()
}

/// Parses a person feed line into its row, validating `roles[].organization_id` and the JSON
/// kinds of the mapped fields, the record's and each role's. The row's edges are
/// [`PersonRow::edges`].
pub fn parse_person_line(line: &[u8], seq: Seq) -> Result<PersonRow, MalformedRecord> {
    let envelope = parse_envelope(line)?;
    let record = envelope.record_text();
    let fields = PersonFields::parse(record)?;
    check_fields(&fields.object, PERSON_FIELDS, None)?;
    for (index, role) in fields.roles.iter().flatten().enumerate() {
        check_fields(&role.object, ROLE_FIELDS, Some(index))?;
    }
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
    fn person_lines_reject_mapped_fields_of_the_wrong_kind() {
        for (record, reason) in [
            (
                r#"{"open_to_work":"yes"}"#,
                "open_to_work: expected a boolean, found a string other than \"true\" or \"false\"",
            ),
            (
                r#"{"id":"abc"}"#,
                "id: expected a number, found a string that is not a number",
            ),
            (
                r#"{"roles":[{"title":"a"},{"title":{"text":"b"}}]}"#,
                "roles[1].title: expected a string, number or boolean, found an object",
            ),
            (
                r#"{"roles":[{"start_date":{"year":2020}}]}"#,
                "roles[0].start_date: expected a string, number or boolean, found an object",
            ),
        ] {
            let line = format!(r#"{{"id":8,"record":{record}}}"#);
            let err = parse_person_line(line.as_bytes(), Seq::default()).unwrap_err();
            assert_eq!(err.kind(), MalformedKind::Record, "{record}");
            assert_eq!(err.reason(), reason, "{record}");
        }
    }

    #[test]
    fn person_lines_with_the_right_kinds_pass() {
        let line = br#"{"id":8,"record":{"id":8,"first_name":"A","skills":["x"],"connections":3,
            "open_to_work":false,"city":10001,"roles":[{"id":1,"title":404,"organization_id":2,
            "start_date":2020,"end_date":null}],
            "organizations":"replaced","memberships":7}}"#;
        assert_eq!(
            parse_person_line(line, Seq::default()).unwrap().org_ids,
            [2]
        );
    }

    #[test]
    fn org_lines_reject_mapped_fields_of_the_wrong_kind() {
        parse_org_line(
            br#"{"id":1,"record":{"name":["A","B"],"keywords":"k","city":3}}"#,
            Seq::default(),
        )
        .unwrap();
        let err = parse_org_line(
            br#"{"id":1,"record":{"name":{"legal":"A"},"keywords":["k"]}}"#,
            Seq::default(),
        )
        .unwrap_err();
        assert_eq!(err.kind(), MalformedKind::Record);
        assert_eq!(
            err.reason(),
            "name: expected a string, number or boolean, found an object"
        );
    }

    #[test]
    fn org_lines_keep_the_last_of_a_repeated_key() {
        for (record, body) in [
            (r#"{"name":"A","name":"B"}"#, r#"{"name":"B"}"#),
            // The checked occurrence is the one kept, so the object never reaches the index.
            (r#"{"name":{"x":1},"name":"A"}"#, r#"{"name":"A"}"#),
            // Keys compare decoded.
            (r#"{"name":"A","\u006eame":"B"}"#, r#"{"name":"B"}"#),
            (
                r#"{"a":1, "id":7,"x":{"deep":[1]},"a":1.50,"tab\tkey":"t","id":"7"}"#,
                r#"{"x":{"deep":[1]},"a":1.50,"tab\tkey":"t","id":"7"}"#,
            ),
        ] {
            let line = format!(r#"{{"id":1,"record":{record}}}"#);
            let row = parse_org_line(line.as_bytes(), Seq::default()).unwrap();
            assert_eq!(String::from_utf8(row.body).unwrap(), body, "{record}");
        }
        // A long discarded value leaves no capacity behind the budget's back.
        let line = format!(
            r#"{{"id":1,"record":{{"x":"{}","x":0}}}}"#,
            "a".repeat(1 << 20)
        );
        let row = parse_org_line(line.as_bytes(), Seq::default()).unwrap();
        assert_eq!(row.body, br#"{"x":0}"#);
        assert_eq!(row.body.capacity(), row.body.len());
        // The kind check still reads the last occurrence, and quarantines it when it is wrong.
        let err = parse_org_line(
            br#"{"id":1,"record":{"name":"A","name":{"x":1}}}"#,
            Seq::default(),
        )
        .unwrap_err();
        assert_eq!(
            err.reason(),
            "name: expected a string, number or boolean, found an object"
        );
    }

    #[test]
    fn org_lines_do_not_read_roles() {
        // An organization with a field named roles is still just an object.
        assert!(parse_org_line(br#"{"id":1,"record":{"roles":7}}"#, Seq::default()).is_ok());
    }
}
