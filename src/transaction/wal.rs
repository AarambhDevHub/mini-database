use crate::transaction::WriteOperation;
use crate::utils::error::{DatabaseError, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tracing::{debug, error, info};

/// Write-Ahead Log record types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LogRecordType {
    Begin,
    Commit,
    Abort,
    Write,
    Checkpoint,
    EndCheckpoint,
}

/// Write-Ahead Log record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogRecord {
    /// Log Sequence Number (LSN)
    pub lsn: u64,

    /// Transaction ID
    pub transaction_id: String,

    /// Record type
    pub record_type: LogRecordType,

    /// Timestamp
    pub timestamp: chrono::DateTime<chrono::Utc>,

    /// Write operation details (for Write records)
    pub write_operation: Option<WriteOperation>,

    /// Previous LSN for this transaction (for recovery)
    pub prev_lsn: Option<u64>,
}

/// Write-Ahead Log for durability and recovery
pub struct WriteAheadLog {
    /// Log file
    log_file: Arc<Mutex<File>>,

    /// Current LSN (Log Sequence Number)
    current_lsn: Arc<Mutex<u64>>,

    /// Log file path
    log_path: PathBuf,

    /// Force sync on each write
    force_sync: bool,
}

impl WriteAheadLog {
    /// Create a new Write-Ahead Log
    pub async fn new(log_path: PathBuf, force_sync: bool) -> Result<Self> {
        info!("Initializing Write-Ahead Log at {:?}", log_path);

        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&log_path)
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to open WAL file: {}", e)))?;

        let mut wal = Self {
            log_file: Arc::new(Mutex::new(log_file)),
            current_lsn: Arc::new(Mutex::new(0)),
            log_path,
            force_sync,
        };

        // Initialize current LSN by reading existing log
        wal.initialize_lsn().await?;

        debug!(
            "Write-Ahead Log initialized with LSN: {}",
            wal.get_current_lsn().await
        );
        Ok(wal)
    }

    /// Initialize current LSN by reading existing log file
    async fn initialize_lsn(&mut self) -> Result<()> {
        let mut file = File::open(&self.log_path)
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to open log for reading: {}", e)))?;

        let mut max_lsn = 0u64;
        let mut buffer = Vec::new();

        file.read_to_end(&mut buffer)
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to read log file: {}", e)))?;

        if !buffer.is_empty() {
            let mut cursor = 0;

            while cursor < buffer.len() {
                // Read record length (4 bytes)
                if cursor + 4 > buffer.len() {
                    break;
                }

                let record_len = u32::from_le_bytes([
                    buffer[cursor],
                    buffer[cursor + 1],
                    buffer[cursor + 2],
                    buffer[cursor + 3],
                ]) as usize;

                cursor += 4;

                if cursor + record_len > buffer.len() {
                    break;
                }

                // Deserialize record
                let record_data = &buffer[cursor..cursor + record_len];
                if let Ok(record) = bincode::deserialize::<LogRecord>(record_data) {
                    max_lsn = max_lsn.max(record.lsn);
                }

                cursor += record_len;
            }
        }

        *self.current_lsn.lock().await = max_lsn;
        Ok(())
    }

    /// Get current LSN
    pub async fn get_current_lsn(&self) -> u64 {
        *self.current_lsn.lock().await
    }

    /// Write a log record
    pub async fn write_record(&self, mut record: LogRecord) -> Result<u64> {
        // Assign LSN
        let mut current_lsn = self.current_lsn.lock().await;
        *current_lsn += 1;
        record.lsn = *current_lsn;
        drop(current_lsn);

        // Serialize record
        let serialized = bincode::serialize(&record).map_err(|e| {
            DatabaseError::Serialization(format!("Failed to serialize log record: {}", e))
        })?;

        // Create record with length prefix
        let mut record_data = Vec::with_capacity(4 + serialized.len());
        record_data.extend_from_slice(&(serialized.len() as u32).to_le_bytes());
        record_data.extend_from_slice(&serialized);

        // Write to file
        let mut file = self.log_file.lock().await;
        file.write_all(&record_data)
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to write log record: {}", e)))?;

        if self.force_sync {
            file.sync_all()
                .await
                .map_err(|e| DatabaseError::Io(format!("Failed to sync log: {}", e)))?;
        }

        debug!(
            "Wrote log record LSN {} for transaction {}",
            record.lsn, record.transaction_id
        );
        Ok(record.lsn)
    }

    /// Log transaction begin
    pub async fn log_begin(&self, transaction_id: String) -> Result<u64> {
        let record = LogRecord {
            lsn: 0, // Will be assigned in write_record
            transaction_id,
            record_type: LogRecordType::Begin,
            timestamp: chrono::Utc::now(),
            write_operation: None,
            prev_lsn: None,
        };

        self.write_record(record).await
    }

    /// Log transaction commit
    pub async fn log_commit(&self, transaction_id: String) -> Result<u64> {
        let record = LogRecord {
            lsn: 0,
            transaction_id,
            record_type: LogRecordType::Commit,
            timestamp: chrono::Utc::now(),
            write_operation: None,
            prev_lsn: None,
        };

        self.write_record(record).await
    }

    /// Log transaction abort
    pub async fn log_abort(&self, transaction_id: String) -> Result<u64> {
        let record = LogRecord {
            lsn: 0,
            transaction_id,
            record_type: LogRecordType::Abort,
            timestamp: chrono::Utc::now(),
            write_operation: None,
            prev_lsn: None,
        };

        self.write_record(record).await
    }

    /// Log write operation
    pub async fn log_write(
        &self,
        transaction_id: String,
        write_operation: WriteOperation,
        prev_lsn: Option<u64>,
    ) -> Result<u64> {
        let record = LogRecord {
            lsn: 0,
            transaction_id,
            record_type: LogRecordType::Write,
            timestamp: chrono::Utc::now(),
            write_operation: Some(write_operation),
            prev_lsn,
        };

        self.write_record(record).await
    }

    /// Log checkpoint start
    pub async fn log_checkpoint(&self) -> Result<u64> {
        let record = LogRecord {
            lsn: 0,
            transaction_id: "SYSTEM".to_string(),
            record_type: LogRecordType::Checkpoint,
            timestamp: chrono::Utc::now(),
            write_operation: None,
            prev_lsn: None,
        };

        self.write_record(record).await
    }

    /// Log checkpoint end
    pub async fn log_checkpoint_end(&self) -> Result<u64> {
        let record = LogRecord {
            lsn: 0,
            transaction_id: "SYSTEM".to_string(),
            record_type: LogRecordType::EndCheckpoint,
            timestamp: chrono::Utc::now(),
            write_operation: None,
            prev_lsn: None,
        };

        self.write_record(record).await
    }

    /// Force sync log to disk
    pub async fn sync(&self) -> Result<()> {
        let file = self.log_file.lock().await;
        file.sync_all()
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to sync log: {}", e)))?;
        Ok(())
    }

    /// Read all log records (for recovery)
    pub async fn read_all_records(&self) -> Result<Vec<LogRecord>> {
        let mut file = File::open(&self.log_path)
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to open log for reading: {}", e)))?;

        let mut records = Vec::new();
        let mut buffer = Vec::new();

        file.read_to_end(&mut buffer)
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to read log file: {}", e)))?;

        let mut cursor = 0;

        while cursor < buffer.len() {
            // Read record length (4 bytes)
            if cursor + 4 > buffer.len() {
                break;
            }

            let record_len = u32::from_le_bytes([
                buffer[cursor],
                buffer[cursor + 1],
                buffer[cursor + 2],
                buffer[cursor + 3],
            ]) as usize;

            cursor += 4;

            if cursor + record_len > buffer.len() {
                error!("Truncated log record at cursor {}", cursor);
                break;
            }

            // Deserialize record
            let record_data = &buffer[cursor..cursor + record_len];
            match bincode::deserialize::<LogRecord>(record_data) {
                Ok(record) => {
                    records.push(record);
                }
                Err(e) => {
                    error!("Failed to deserialize log record: {}", e);
                    break;
                }
            }

            cursor += record_len;
        }

        debug!("Read {} log records", records.len());
        Ok(records)
    }

    /// Truncate log (for recovery after successful checkpoint)
    pub async fn truncate_before_lsn(&self, lsn: u64) -> Result<()> {
        let records = self.read_all_records().await?;
        let remaining_records: Vec<_> = records
            .into_iter()
            .filter(|record| record.lsn >= lsn)
            .collect();

        // Rewrite log file with remaining records
        let mut new_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.log_path)
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to truncate log: {}", e)))?;

        for record in remaining_records {
            let serialized = bincode::serialize(&record).map_err(|e| {
                DatabaseError::Serialization(format!("Failed to serialize record: {}", e))
            })?;

            let mut record_data = Vec::with_capacity(4 + serialized.len());
            record_data.extend_from_slice(&(serialized.len() as u32).to_le_bytes());
            record_data.extend_from_slice(&serialized);

            new_file
                .write_all(&record_data)
                .await
                .map_err(|e| DatabaseError::Io(format!("Failed to write record: {}", e)))?;
        }

        new_file
            .sync_all()
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to sync truncated log: {}", e)))?;

        // Replace log file
        *self.log_file.lock().await = new_file;

        info!("Truncated log before LSN {}", lsn);
        Ok(())
    }
}
