use std::borrow::Cow;

use sqlx::PgPool;
use thiserror::Error;
use tokio_postgres::types::Oid;

use crate::types::TableId;

/// Maximum length for a Postgres replication slot name in bytes.
const MAX_SLOT_NAME_LENGTH: usize = 63;

/// Default prefix for replication slot names.
pub const DEFAULT_SLOT_PREFIX: &str = "supabase_etl";

/// Suffix for apply worker slots.
const APPLY_SUFFIX: &str = "apply";
/// Suffix for table sync worker slots.
const TABLE_SYNC_SUFFIX: &str = "table_sync";

/// Error type for slot operations.
#[derive(Debug, Error)]
pub enum EtlReplicationSlotError {
    #[error("Invalid slot name length: {0}")]
    InvalidSlotNameLength(String),

    #[error("Invalid slot name: {0}")]
    InvalidSlotName(String),
}

/// Parsed representation of a replication slot name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EtlReplicationSlot {
    /// Apply worker slot for a pipeline.
    Apply {
        pipeline_id: u64,
        prefix: Cow<'static, str>,
    },
    /// Table sync worker slot for a pipeline and table.
    TableSync {
        pipeline_id: u64,
        table_id: TableId,
        prefix: Cow<'static, str>,
    },
}

impl EtlReplicationSlot {
    /// Creates a new [`EtlReplicationSlot`] for the apply worker with a custom prefix.
    pub fn for_apply_worker(pipeline_id: u64, prefix: impl Into<Cow<'static, str>>) -> Self {
        Self::Apply {
            pipeline_id,
            prefix: prefix.into(),
        }
    }

    /// Creates a new [`EtlReplicationSlot`] for the table sync worker with a custom prefix.
    pub fn for_table_sync_worker(
        pipeline_id: u64,
        table_id: TableId,
        prefix: impl Into<Cow<'static, str>>,
    ) -> Self {
        Self::TableSync {
            pipeline_id,
            table_id,
            prefix: prefix.into(),
        }
    }

    /// Returns the prefix of apply sync slot for a pipeline.
    pub fn apply_prefix(
        pipeline_id: u64,
        slot_prefix: &str,
    ) -> Result<String, EtlReplicationSlotError> {
        let prefix = format!("{slot_prefix}_{APPLY_SUFFIX}_{pipeline_id}");

        if prefix.len() >= MAX_SLOT_NAME_LENGTH {
            return Err(EtlReplicationSlotError::InvalidSlotNameLength(prefix));
        }

        Ok(prefix)
    }

    /// Returns the prefix of table sync slots for a pipeline.
    pub fn table_sync_prefix(
        pipeline_id: u64,
        slot_prefix: &str,
    ) -> Result<String, EtlReplicationSlotError> {
        let prefix = format!("{slot_prefix}_{TABLE_SYNC_SUFFIX}_{pipeline_id}_");

        if prefix.len() >= MAX_SLOT_NAME_LENGTH {
            return Err(EtlReplicationSlotError::InvalidSlotNameLength(prefix));
        }

        Ok(prefix)
    }
}

impl TryFrom<&str> for EtlReplicationSlot {
    type Error = EtlReplicationSlotError;

    fn try_from(slot_name: &str) -> Result<Self, Self::Error> {
        // Try to find the apply pattern: {prefix}_apply_{pipeline_id}
        let apply_pattern = format!("_{APPLY_SUFFIX}_");
        if let Some(apply_pos) = slot_name.find(&apply_pattern) {
            let prefix = &slot_name[..apply_pos];
            let rest = &slot_name[apply_pos + apply_pattern.len()..];
            let pipeline_id: u64 = rest
                .parse()
                .ok()
                .ok_or_else(|| EtlReplicationSlotError::InvalidSlotName(slot_name.into()))?;

            return Ok(EtlReplicationSlot::for_apply_worker(
                pipeline_id,
                prefix.to_owned(),
            ));
        }

        // Try to find the table_sync pattern: {prefix}_table_sync_{pipeline_id}_{table_id}
        let table_sync_pattern = format!("_{TABLE_SYNC_SUFFIX}_");
        if let Some(sync_pos) = slot_name.find(&table_sync_pattern) {
            let prefix = &slot_name[..sync_pos];
            let rest = &slot_name[sync_pos + table_sync_pattern.len()..];

            let mut parts = rest.rsplitn(2, '_');
            let table_id_str = parts
                .next()
                .ok_or_else(|| EtlReplicationSlotError::InvalidSlotName(slot_name.into()))?;
            let pipeline_id_str = parts
                .next()
                .ok_or_else(|| EtlReplicationSlotError::InvalidSlotName(slot_name.into()))?;

            let pipeline_id: u64 = pipeline_id_str
                .parse()
                .ok()
                .ok_or_else(|| EtlReplicationSlotError::InvalidSlotName(slot_name.into()))?;
            let table_oid: Oid = table_id_str
                .parse()
                .ok()
                .ok_or_else(|| EtlReplicationSlotError::InvalidSlotName(slot_name.into()))?;

            return Ok(EtlReplicationSlot::for_table_sync_worker(
                pipeline_id,
                TableId::new(table_oid),
                prefix.to_owned(),
            ));
        }

        Err(EtlReplicationSlotError::InvalidSlotName(slot_name.into()))
    }
}

impl TryFrom<EtlReplicationSlot> for String {
    type Error = EtlReplicationSlotError;

    fn try_from(slot: EtlReplicationSlot) -> Result<Self, Self::Error> {
        let slot_name = match slot {
            EtlReplicationSlot::Apply {
                pipeline_id,
                prefix,
            } => {
                format!("{prefix}_{APPLY_SUFFIX}_{pipeline_id}")
            }
            EtlReplicationSlot::TableSync {
                pipeline_id,
                table_id,
                prefix,
            } => {
                format!(
                    "{prefix}_{TABLE_SYNC_SUFFIX}_{pipeline_id}_{}",
                    table_id.into_inner()
                )
            }
        };

        if slot_name.len() > MAX_SLOT_NAME_LENGTH {
            return Err(EtlReplicationSlotError::InvalidSlotNameLength(slot_name));
        }

        Ok(slot_name)
    }
}

/// Deletes all replication slots for a given pipeline.
///
/// This function deletes both the apply worker slot and all table sync worker slots
/// for the tables associated with the pipeline.
///
/// If the slot name can't be computed, this function will silently skip the deletion of the slot.
pub async fn delete_pipeline_replication_slots(
    pool: &PgPool,
    pipeline_id: u64,
    table_ids: &[TableId],
    slot_prefix: &str,
) -> sqlx::Result<()> {
    // Collect all slot names that need to be deleted
    let mut slot_names: Vec<String> = Vec::with_capacity(table_ids.len() + 1);

    // Add apply worker slot
    let slot_name = EtlReplicationSlot::for_apply_worker(pipeline_id, slot_prefix.to_owned());
    if let Ok(apply_slot_name) = slot_name.try_into() {
        slot_names.push(apply_slot_name);
    };

    // Add table sync worker slots
    for table_id in table_ids {
        let slot_name =
            EtlReplicationSlot::for_table_sync_worker(pipeline_id, *table_id, slot_prefix.to_owned());
        if let Ok(table_sync_slot_name) = slot_name.try_into() {
            slot_names.push(table_sync_slot_name);
        };
    }

    // Delete only active slots
    let query = String::from(
        r#"
        select pg_drop_replication_slot(r.slot_name)
        from pg_replication_slots r
        where r.slot_name = any($1) and r.active = false;
        "#,
    );
    sqlx::query(&query).bind(slot_names).execute(pool).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_worker_slot_name_default_prefix() {
        let pipeline_id = 1;
        let result: String =
            EtlReplicationSlot::for_apply_worker(pipeline_id, DEFAULT_SLOT_PREFIX)
                .try_into()
                .unwrap();

        assert!(result.starts_with(DEFAULT_SLOT_PREFIX));
        assert!(result.len() <= MAX_SLOT_NAME_LENGTH);
        assert_eq!(result, "supabase_etl_apply_1");
    }

    #[test]
    fn test_apply_worker_slot_name_custom_prefix() {
        let pipeline_id = 1;
        let result: String = EtlReplicationSlot::for_apply_worker(pipeline_id, "myapp_prod")
            .try_into()
            .unwrap();

        assert!(result.starts_with("myapp_prod"));
        assert!(result.len() <= MAX_SLOT_NAME_LENGTH);
        assert_eq!(result, "myapp_prod_apply_1");
    }

    #[test]
    fn test_table_sync_slot_name_default_prefix() {
        let pipeline_id = 1;
        let result: String = EtlReplicationSlot::for_table_sync_worker(
            pipeline_id,
            TableId::new(123),
            DEFAULT_SLOT_PREFIX,
        )
        .try_into()
        .unwrap();

        assert!(result.starts_with(DEFAULT_SLOT_PREFIX));
        assert!(result.len() <= MAX_SLOT_NAME_LENGTH);
        assert_eq!(result, "supabase_etl_table_sync_1_123");
    }

    #[test]
    fn test_table_sync_slot_name_custom_prefix() {
        let pipeline_id = 1;
        let result: String =
            EtlReplicationSlot::for_table_sync_worker(pipeline_id, TableId::new(123), "custom")
                .try_into()
                .unwrap();

        assert!(result.starts_with("custom"));
        assert!(result.len() <= MAX_SLOT_NAME_LENGTH);
        assert_eq!(result, "custom_table_sync_1_123");
    }

    #[test]
    fn test_slot_name_length_validation() {
        // Test that normal slot names are within limits
        // Max u64
        let pipeline_id = 9223372036854775807_u64;
        // Max u32
        let result: Result<String, EtlReplicationSlotError> =
            EtlReplicationSlot::for_table_sync_worker(
                pipeline_id,
                TableId::new(4294967295),
                DEFAULT_SLOT_PREFIX,
            )
            .try_into();
        assert!(result.is_ok());

        let slot_name = result.unwrap();
        assert!(slot_name.len() <= MAX_SLOT_NAME_LENGTH);

        // The longest possible slot name with current prefixes should still be valid
        assert_eq!(
            slot_name,
            "supabase_etl_table_sync_9223372036854775807_4294967295"
        );
        assert!(slot_name.len() <= MAX_SLOT_NAME_LENGTH);
    }

    #[test]
    fn test_apply_sync_slot_prefix() {
        let prefix = EtlReplicationSlot::apply_prefix(42, DEFAULT_SLOT_PREFIX).unwrap();
        assert_eq!(prefix, "supabase_etl_apply_42");
    }

    #[test]
    fn test_apply_sync_slot_prefix_custom() {
        let prefix = EtlReplicationSlot::apply_prefix(42, "myapp").unwrap();
        assert_eq!(prefix, "myapp_apply_42");
    }

    #[test]
    fn test_table_sync_slot_prefix() {
        let prefix = EtlReplicationSlot::table_sync_prefix(42, DEFAULT_SLOT_PREFIX).unwrap();
        assert_eq!(prefix, "supabase_etl_table_sync_42_");
    }

    #[test]
    fn test_table_sync_slot_prefix_custom() {
        let prefix = EtlReplicationSlot::table_sync_prefix(42, "myapp").unwrap();
        assert_eq!(prefix, "myapp_table_sync_42_");
    }

    #[test]
    fn test_parse_apply_slot_default_prefix() {
        let parsed = EtlReplicationSlot::try_from("supabase_etl_apply_13").unwrap();
        assert_eq!(
            parsed,
            EtlReplicationSlot::Apply {
                pipeline_id: 13,
                prefix: Cow::Owned("supabase_etl".to_string()),
            }
        );
    }

    #[test]
    fn test_parse_apply_slot_custom_prefix() {
        let parsed = EtlReplicationSlot::try_from("myapp_prod_apply_42").unwrap();
        assert_eq!(
            parsed,
            EtlReplicationSlot::Apply {
                pipeline_id: 42,
                prefix: Cow::Owned("myapp_prod".to_string()),
            }
        );
    }

    #[test]
    fn test_parse_table_sync_slot_default_prefix() {
        let parsed = EtlReplicationSlot::try_from("supabase_etl_table_sync_7_12345").unwrap();
        assert_eq!(
            parsed,
            EtlReplicationSlot::TableSync {
                pipeline_id: 7,
                table_id: TableId::new(12345_u32),
                prefix: Cow::Owned("supabase_etl".to_string()),
            }
        );
    }

    #[test]
    fn test_parse_table_sync_slot_custom_prefix() {
        let parsed = EtlReplicationSlot::try_from("custom_table_sync_7_12345").unwrap();
        assert_eq!(
            parsed,
            EtlReplicationSlot::TableSync {
                pipeline_id: 7,
                table_id: TableId::new(12345_u32),
                prefix: Cow::Owned("custom".to_string()),
            }
        );
    }

    #[test]
    fn test_parse_invalid_slot() {
        assert!(EtlReplicationSlot::try_from("unknown_slot").is_err());
        assert!(EtlReplicationSlot::try_from("supabase_etl_apply_").is_err());
        assert!(EtlReplicationSlot::try_from("supabase_etl_table_sync_abc").is_err());
    }

    #[test]
    fn test_roundtrip_slot_names() {
        // Test that parsing a slot name and converting back produces the same result
        let original = "myprefix_apply_123";
        let parsed = EtlReplicationSlot::try_from(original).unwrap();
        let result: String = parsed.try_into().unwrap();
        assert_eq!(result, original);

        let original = "myprefix_table_sync_456_789";
        let parsed = EtlReplicationSlot::try_from(original).unwrap();
        let result: String = parsed.try_into().unwrap();
        assert_eq!(result, original);
    }
}
