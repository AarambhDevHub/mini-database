use crate::core::DatabaseConfig;
use crate::utils::error::{DatabaseError, Result};
use memmap2::MmapOptions;
use std::io::{BufRead, BufReader, SeekFrom};
use std::path::Path;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tracing::{debug, trace};

/// Ultra-fast buffered file I/O with memory-mapped operations
pub struct FileReader {
    config: Arc<DatabaseConfig>,
}

impl FileReader {
    /// Create a new file reader
    pub fn new(config: Arc<DatabaseConfig>) -> Result<Self> {
        Ok(Self { config })
    }

    /// Read entire file with automatic optimization (mmap vs buffered)
    pub async fn read_file<P: AsRef<Path>>(&self, path: P) -> Result<Vec<u8>> {
        let path = path.as_ref();
        let metadata = tokio::fs::metadata(path)
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to read metadata: {}", e)))?;

        let file_size = metadata.len() as usize;

        if file_size >= self.config.mmap_threshold {
            debug!(
                "Using memory-mapped read for file: {:?} (size: {})",
                path, file_size
            );
            self.read_mmap(path).await
        } else {
            debug!(
                "Using buffered read for file: {:?} (size: {})",
                path, file_size
            );
            self.read_buffered(path).await
        }
    }

    /// Memory-mapped file reading for large files
    pub async fn read_mmap<P: AsRef<Path>>(&self, path: P) -> Result<Vec<u8>> {
        let path = path.as_ref();

        let file = std::fs::File::open(path)
            .map_err(|e| DatabaseError::Io(format!("Failed to open file: {}", e)))?;

        let mmap = unsafe { MmapOptions::new().map(&file) }
            .map_err(|e| DatabaseError::Io(format!("Failed to mmap file: {}", e)))?;

        trace!("Memory-mapped {} bytes from {:?}", mmap.len(), path);
        Ok(mmap.to_vec())
    }

    /// Buffered file reading for smaller files
    pub async fn read_buffered<P: AsRef<Path>>(&self, path: P) -> Result<Vec<u8>> {
        let mut file = File::open(path)
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to open file: {}", e)))?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to read file: {}", e)))?;

        trace!("Read {} bytes using buffered I/O", buffer.len());
        Ok(buffer)
    }

    /// Read file chunk with offset and length
    pub async fn read_chunk<P: AsRef<Path>>(
        &self,
        path: P,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>> {
        let mut file = File::open(path)
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to open file: {}", e)))?;

        file.seek(SeekFrom::Start(offset))
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to seek file: {}", e)))?;

        let mut buffer = vec![0; length];
        file.read_exact(&mut buffer)
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to read chunk: {}", e)))?;

        trace!("Read chunk: offset={}, length={}", offset, length);
        Ok(buffer)
    }

    /// Read file lines efficiently
    pub fn read_lines<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> Result<impl Iterator<Item = Result<String>>> {
        let file = std::fs::File::open(path)
            .map_err(|e| DatabaseError::Io(format!("Failed to open file: {}", e)))?;

        let reader = BufReader::new(file);
        Ok(reader
            .lines()
            .map(|line| line.map_err(|e| DatabaseError::Io(format!("Failed to read line: {}", e)))))
    }

    /// Check if file exists
    pub async fn exists<P: AsRef<Path>>(&self, path: P) -> bool {
        tokio::fs::metadata(path).await.is_ok()
    }

    /// Get file size
    pub async fn file_size<P: AsRef<Path>>(&self, path: P) -> Result<u64> {
        let metadata = tokio::fs::metadata(path)
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to read metadata: {}", e)))?;
        Ok(metadata.len())
    }
}
