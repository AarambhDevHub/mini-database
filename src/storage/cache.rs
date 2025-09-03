use dashmap::DashMap;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use tracing::{debug, trace};

/// LRU memory cache for frequently accessed data
pub struct Cache {
    data_cache: Arc<Mutex<LruCache<String, Vec<u8>>>>,
    index_cache: DashMap<String, u64>,
    stats: Arc<Mutex<CacheStats>>,
}

#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub current_size: usize,
    pub capacity: usize,
}

impl CacheStats {
    pub fn hit_rate(&self) -> f64 {
        if self.hits + self.misses == 0 {
            0.0
        } else {
            self.hits as f64 / (self.hits + self.misses) as f64
        }
    }
}

impl Cache {
    /// Create a new cache with specified capacity in bytes
    pub fn new(capacity_bytes: usize) -> Self {
        let capacity =
            NonZeroUsize::new(capacity_bytes / 1024).unwrap_or(NonZeroUsize::new(1).unwrap());

        debug!("Creating cache with capacity: {} KB", capacity);

        Self {
            data_cache: Arc::new(Mutex::new(LruCache::new(capacity))),
            index_cache: DashMap::new(),
            stats: Arc::new(Mutex::new(CacheStats {
                capacity: capacity_bytes,
                ..Default::default()
            })),
        }
    }

    /// Get data from cache
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let mut cache = self.data_cache.lock().unwrap();
        let result = cache.get(key).cloned();

        let mut stats = self.stats.lock().unwrap();
        if result.is_some() {
            stats.hits += 1;
            trace!("Cache hit for key: {}", key);
        } else {
            stats.misses += 1;
            trace!("Cache miss for key: {}", key);
        }

        result
    }

    /// Put data into cache
    pub fn put(&self, key: String, value: Vec<u8>) {
        let value_size = value.len();
        let mut cache = self.data_cache.lock().unwrap();

        if let Some(_evicted) = cache.put(key.clone(), value) {
            let mut stats = self.stats.lock().unwrap();
            stats.evictions += 1;
            debug!("Cache eviction occurred for key: {}", key);
        }

        let mut stats = self.stats.lock().unwrap();
        stats.current_size += value_size;

        trace!("Cached {} bytes for key: {}", value_size, key);
    }

    /// Get index from cache
    pub fn get_index(&self, key: &str) -> Option<u64> {
        let result = self.index_cache.get(key).map(|v| *v);
        trace!("Index cache lookup for key: {} -> {:?}", key, result);
        result
    }

    /// Put index into cache
    pub fn put_index(&self, key: String, offset: u64) {
        self.index_cache.insert(key.clone(), offset);
        trace!("Cached index for key: {} -> {}", key, offset);
    }

    /// Remove entry from cache
    pub fn remove(&self, key: &str) {
        let mut cache = self.data_cache.lock().unwrap();
        if let Some(removed) = cache.pop(key) {
            let mut stats = self.stats.lock().unwrap();
            stats.current_size = stats.current_size.saturating_sub(removed.len());
            debug!(
                "Removed {} bytes from cache for key: {}",
                removed.len(),
                key
            );
        }

        self.index_cache.remove(key);
    }

    /// Clear all cache entries
    pub fn clear(&self) {
        let mut cache = self.data_cache.lock().unwrap();
        cache.clear();
        self.index_cache.clear();

        let mut stats = self.stats.lock().unwrap();
        stats.current_size = 0;
        stats.evictions = 0;

        debug!("Cache cleared");
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        self.stats.lock().unwrap().clone()
    }

    /// Get current cache size
    pub fn len(&self) -> usize {
        let cache = self.data_cache.lock().unwrap();
        cache.len()
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_operations() {
        let cache = Cache::new(1024);

        // Test put and get
        cache.put("key1".to_string(), b"value1".to_vec());
        assert_eq!(cache.get("key1"), Some(b"value1".to_vec()));

        // Test miss
        assert_eq!(cache.get("nonexistent"), None);

        // Test stats
        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert!(stats.hit_rate() > 0.0);
    }
}
