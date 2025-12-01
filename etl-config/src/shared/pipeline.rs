use serde::{Deserialize, Serialize};

use crate::shared::{
    PgConnectionConfig, PgConnectionConfigWithoutSecrets, ValidationError, batch::BatchConfig,
};

/// Maximum length for a custom slot prefix.
///
/// PostgreSQL limits slot names to 63 bytes. With the longest suffix pattern
/// `_table_sync_{max_u64}_{max_u32}` = 40 chars, we allow prefixes up to 20 chars.
pub const MAX_SLOT_PREFIX_LENGTH: usize = 20;

/// Configuration for an ETL pipeline.
///
/// Contains all settings required to run a replication pipeline including
/// source database connection, batching parameters, and worker limits.
///
/// This intentionally does not implement [`Serialize`] to avoid accidentally
/// leaking secrets in the config into serialized forms.
#[derive(Clone, Debug, Deserialize)]
pub struct PipelineConfig {
    /// The unique identifier for this pipeline.
    ///
    /// A pipeline id determines isolation between pipelines, in terms of replication slots and state
    /// store.
    pub id: u64,
    /// Optional prefix to use for replication slots.
    ///
    /// Defaults to "supabase_etl".
    #[serde(default)]
    pub slot_prefix: Option<String>,
    /// Name of the Postgres publication to use for logical replication.
    pub publication_name: String,
    /// The connection configuration for the Postgres instance to which the pipeline connects for
    /// replication.
    pub pg_connection: PgConnectionConfig,
    /// Batch processing configuration.
    pub batch: BatchConfig,
    /// Number of milliseconds between one retry and another when a table error occurs.
    pub table_error_retry_delay_ms: u64,
    /// Maximum number of automatic retry attempts before requiring manual intervention.
    pub table_error_retry_max_attempts: u32,
    /// Maximum number of table sync workers that can run at a time
    pub max_table_sync_workers: u16,
}

impl PipelineConfig {
    /// Validates pipeline configuration settings.
    ///
    /// Checks connection settings and ensures worker count is non-zero.
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.pg_connection.tls.validate()?;

        if self.max_table_sync_workers == 0 {
            return Err(ValidationError::MaxTableSyncWorkersZero);
        }

        if self.table_error_retry_max_attempts == 0 {
            return Err(ValidationError::TableErrorRetryMaxAttemptsZero);
        }

        if self
            .slot_prefix
            .as_ref()
            .is_some_and(|prefix| prefix.is_empty())
        {
            return Err(ValidationError::SlotPrefixEmpty);
        }

        if self
            .slot_prefix
            .as_ref()
            .is_some_and(|prefix| prefix.len() > MAX_SLOT_PREFIX_LENGTH)
        {
            return Err(ValidationError::SlotPrefixTooLong {
                max_length: MAX_SLOT_PREFIX_LENGTH,
            });
        }

        Ok(())
    }
}

/// Same as [`PipelineConfig`] but without secrets. This type
/// implements [`Serialize`] because it does not contains secrets
/// so is safe to serialize.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PipelineConfigWithoutSecrets {
    /// The unique identifier for this pipeline.
    ///
    /// A pipeline id determines isolation between pipelines, in terms of replication slots and state
    /// store.
    pub id: u64,
    /// Optional prefix to use for replication slots.
    ///
    /// Defaults to "supabase_etl".
    #[serde(default)]
    pub slot_prefix: Option<String>,
    /// Name of the Postgres publication to use for logical replication.
    pub publication_name: String,
    /// The connection configuration for the Postgres instance to which the pipeline connects for
    /// replication.
    pub pg_connection: PgConnectionConfigWithoutSecrets,
    /// Batch processing configuration.
    pub batch: BatchConfig,
    /// Number of milliseconds between one retry and another when a table error occurs.
    pub table_error_retry_delay_ms: u64,
    /// Maximum number of automatic retry attempts before requiring manual intervention.
    pub table_error_retry_max_attempts: u32,
    /// Maximum number of table sync workers that can run at a time
    pub max_table_sync_workers: u16,
}

impl From<PipelineConfig> for PipelineConfigWithoutSecrets {
    fn from(value: PipelineConfig) -> Self {
        PipelineConfigWithoutSecrets {
            id: value.id,
            slot_prefix: value.slot_prefix,
            publication_name: value.publication_name,
            pg_connection: value.pg_connection.into(),
            batch: value.batch,
            table_error_retry_delay_ms: value.table_error_retry_delay_ms,
            table_error_retry_max_attempts: value.table_error_retry_max_attempts,
            max_table_sync_workers: value.max_table_sync_workers,
        }
    }
}
