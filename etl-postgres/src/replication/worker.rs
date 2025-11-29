use std::borrow::Cow;

use crate::replication::slots::EtlReplicationSlot;
use crate::types::TableId;

/// Enum representing the types of workers that can be involved with a replication task.
#[derive(Debug, Copy, Clone)]
pub enum WorkerType {
    Apply,
    TableSync { table_id: TableId },
}

impl WorkerType {
    pub fn build_etl_replication_slot(
        &self,
        pipeline_id: u64,
        slot_prefix: impl Into<Cow<'static, str>>,
    ) -> EtlReplicationSlot {
        let prefix = slot_prefix.into();
        match self {
            Self::Apply => EtlReplicationSlot::Apply {
                pipeline_id,
                prefix,
            },
            Self::TableSync { table_id } => EtlReplicationSlot::TableSync {
                pipeline_id,
                table_id: *table_id,
                prefix,
            },
        }
    }
}
