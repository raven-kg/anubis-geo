use std::{env, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use anyhow::Context;
use ipnet::IpNet;
use maxminddb::{geoip2, Reader};
use moka::future::Cache;
use tokio::signal;
use tokio::sync::RwLock;
use tonic::{transport::Server, Request, Response, Status, metadata::MetadataMap};
use tonic::service::Interceptor;
use tonic::codec::CompressionEncoding;
use tracing::{info, warn, error, debug};
use sqlx::{Any, Postgres, Sqlite, AnyPool, Row, Database, any::AnyPoolOptions};
use tokio_cron_scheduler::{JobScheduler, Job};
use tonic_reflection::server::Builder as ReflectionBuilder;
use bzip2::read::BzDecoder;

pub mod techaro {
    pub mod thoth {
        pub mod iptoasn {
            pub mod v1 {
                tonic::include_proto!("techaro.thoth.iptoasn.v1");
            }
        }
        pub mod reputation {
            pub mod v1 {
                tonic::include_proto!("techaro.thoth.reputation.v1");
            }
        }
    }
}

use techaro::thoth::iptoasn::v1::{
    ip_to_asn_service_server::{IpToAsnService, IpToAsnServiceServer},
    LookupRequest, LookupResponse,
};
use techaro::thoth::reputation::v1::{
    repute_service_server::{ReputeService, ReputeServiceServer},
    ReputeServiceQueryRequest, ReputeServiceQueryResponse, IpList,
};

/// Database type enum
#[derive(Debug, Clone, Copy)]
enum DatabaseType {
    Sqlite,
    Postgres,
}

/// Simple row type for BGP data
#[derive(Debug, Clone)]
struct BgpRow {
    prefix: String,
    asn: i64,
    description: String,
    country_code: String,
    registry: String,
    last_updated: String,
}

impl BgpRow {
    fn to_bgp_prefix(&self) -> Result<BgpPrefix, anyhow::Error> {
        let prefix = self.prefix.parse::<IpNet>()?;
        Ok(BgpPrefix {
            prefix,
            asn: self.asn as u32,
            description: self.description.clone(),
            country_code: self.country_code.clone(),
            registry: self.registry.clone(),
            last_updated: chrono::DateTime::parse_from_rfc3339(&self.last_updated)?.with_timezone(&chrono::Utc),
        })
    }
}

/// BGP prefix information stored in database
#[derive(Clone, Debug)]
struct BgpPrefix {
    prefix: IpNet,
    asn: u32,
    description: String,
    country_code: String,
    registry: String,
    last_updated: chrono::DateTime<chrono::Utc>,
}

/// Cached BGP lookup result with TTL
#[derive(Clone, Debug)]
struct CachedBgpResult {
    prefix: Option<BgpPrefix>,
    cached_at: std::time::Instant,
    ttl: Duration,
}

impl CachedBgpResult {
    fn new(prefix: Option<BgpPrefix>, ttl: Duration) -> Self {
        Self {
            prefix,
            cached_at: std::time::Instant::now(),
            ttl,
        }
    }

    fn is_expired(&self) -> bool {
        self.cached_at.elapsed() > self.ttl
    }
}

/// BGP data storage and caching layer
#[derive(Clone)]
struct BgpStorage {
    pool: sqlx::AnyPool,
    db_type: DatabaseType,
    client: reqwest::Client,
    /// Cache for IP -> BGP prefix lookups (short-lived, 5-10 minutes)
    lookup_cache: Cache<std::net::IpAddr, CachedBgpResult>,
}

impl BgpStorage {
    /// Initialize BGP storage with database
    async fn new(database_uri: &str) -> anyhow::Result<Self> {
        let (pool, db_type) = if database_uri.starts_with("postgres://") || database_uri.starts_with("postgresql://") {
            info!("Connecting to PostgreSQL database: {}", database_uri);
            let pool = sqlx::AnyPool::connect(database_uri).await?;
            (pool, DatabaseType::Postgres)
        } else {
            info!("Connecting to SQLite database: {}", database_uri);
            if database_uri.starts_with("sqlite:") {
                let path = database_uri.trim_start_matches("sqlite:");
                if !std::path::Path::new(path).exists() {
                    if let Some(parent) = std::path::Path::new(path).parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    std::fs::File::create(path)?;
                    info!("Created SQLite database file: {}", path);
                }
            }

            let pool = sqlx::AnyPool::connect(database_uri).await?;
            (pool, DatabaseType::Sqlite)
        };

        let storage = Self {
            pool,
            db_type,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .user_agent("GeoIP-BGP-Service/1.0")
                .build()?,
            lookup_cache: Cache::builder()
                .time_to_live(Duration::from_secs(300))
                .max_capacity(10_000)
                .build(),
        };

        // Initialize database schema
        storage.init_db().await?;

        // Populate initial data
        if storage.check_table().await? {
            info!("Starting initial BGP data population...");
            storage.update_bgp_data().await?;
        }

        Ok(storage)
    }

    /// Initialize database schema
    async fn init_db(&self) -> anyhow::Result<()> {
        info!("Initializing database schema...");

        match self.db_type {
            DatabaseType::Postgres => {
                let statements = [
                    r#"
                CREATE TABLE IF NOT EXISTS bgp_prefixes (
                    id SERIAL PRIMARY KEY,
                    prefix TEXT NOT NULL UNIQUE,
                    asn INTEGER NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    country_code TEXT NOT NULL DEFAULT '',
                    registry TEXT NOT NULL DEFAULT '',
                    last_updated TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                )
                "#,
                    "CREATE INDEX IF NOT EXISTS idx_bgp_prefixes_prefix ON bgp_prefixes (prefix)",
                    "CREATE INDEX IF NOT EXISTS idx_bgp_prefixes_asn ON bgp_prefixes (asn)",
                    "CREATE INDEX IF NOT EXISTS idx_bgp_prefixes_updated ON bgp_prefixes (last_updated)",
                    "CREATE INDEX IF NOT EXISTS idx_bgp_prefixes_country ON bgp_prefixes (country_code)",
                ];

                for statement in statements {
                    sqlx::query(statement).execute(&self.pool).await?;
                }
            }
            DatabaseType::Sqlite => {
                let create_table_sql = r#"
                CREATE TABLE IF NOT EXISTS bgp_prefixes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    prefix TEXT NOT NULL UNIQUE,
                    asn INTEGER NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    country_code TEXT NOT NULL DEFAULT '',
                    registry TEXT NOT NULL DEFAULT '',
                    last_updated TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                );

                CREATE INDEX IF NOT EXISTS idx_bgp_prefixes_prefix ON bgp_prefixes (prefix);
                CREATE INDEX IF NOT EXISTS idx_bgp_prefixes_asn ON bgp_prefixes (asn);
                CREATE INDEX IF NOT EXISTS idx_bgp_prefixes_updated ON bgp_prefixes (last_updated);
                CREATE INDEX IF NOT EXISTS idx_bgp_prefixes_country ON bgp_prefixes (country_code);
            "#;
                sqlx::query(create_table_sql).execute(&self.pool).await?;
            }
        }

        info!("Database schema initialized successfully");
        Ok(())
    }

    /// Check if we should populate initial BGP data
    async fn check_table(&self) -> anyhow::Result<bool> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM bgp_prefixes")
            .fetch_one(&self.pool)
            .await?;

        if count == 0 {
            info!("Database is empty, will populate with initial BGP data");
            Ok(true)
        } else {
            info!("Database contains {} BGP prefixes", count);
            Ok(false)
        }
    }
    /// Find the most specific BGP prefix for an IP address
    async fn find_prefix(&self, ip: std::net::IpAddr) -> anyhow::Result<Option<BgpPrefix>> {
        // Check cache first
        if let Some(cached) = self.lookup_cache.get(&ip).await {
            if !cached.is_expired() {
                debug!("BGP cache hit for {}", ip);
                return Ok(cached.prefix);
            }
        }

        debug!("BGP cache miss for {}, querying database", ip);

        // Query database for matching prefixes
        let ip_str = ip.to_string();
        let query = match self.db_type {
            DatabaseType::Sqlite =>
                r#"
                SELECT prefix, asn, description, country_code, registry, last_updated
                FROM bgp_prefixes
                WHERE ? BETWEEN inet_aton(substr(prefix, 1, instr(prefix, '/') - 1))
                    AND inet_aton(substr(prefix, 1, instr(prefix, '/') - 1)) +
                        (CASE WHEN substr(prefix, instr(prefix, '/') + 1) = '32' THEN 0
                              ELSE (1 << (32 - CAST(substr(prefix, instr(prefix, '/') + 1) AS INTEGER))) - 1 END)
                ORDER BY CAST(substr(prefix, instr(prefix, '/') + 1) AS INTEGER) DESC
                LIMIT 1
                "#,
            DatabaseType::Postgres =>
                r#"
                SELECT prefix, asn, description, country_code, registry, last_updated
                FROM bgp_prefixes
                WHERE inet(?) <<= inet(prefix)
                ORDER BY masklen(inet(prefix)) DESC
                LIMIT 1
                "#,
        };

        let rows: Vec<(String, i64, String, String, String, String)> = sqlx::query_as(query)
            .bind(&ip_str)
            .fetch_all(&self.pool)
            .await?;

        let bgp_rows: Vec<BgpRow> = rows.into_iter().map(|(prefix, asn, desc, cc, reg, updated)| BgpRow { prefix, asn, description: desc, country_code: cc, registry: reg, last_updated: updated }).collect();
        let prefix = if let Some(row) = bgp_rows.first() {
            match row.to_bgp_prefix() {
                Ok(prefix) => Some(prefix),
                Err(e) => {
                    warn!("Failed to parse BGP prefix from row: {}", e);
                    None
                }
            }
        } else {
            None
        };

        // If no result in DB, try to fetch from external APIs
        let final_prefix = if prefix.is_none() {
            self.fetch_and_store_prefix(ip).await?
        } else {
            prefix
        };

        // Cache the result (even if None)
        let cached_result = CachedBgpResult::new(final_prefix.clone(), Duration::from_secs(300));
        self.lookup_cache.insert(ip, cached_result).await;

        Ok(final_prefix)
    }

    /// Simplified find_prefix method
    async fn find_prefix_simple(&self, ip: std::net::IpAddr) -> Option<BgpPrefix> {
        match self.find_prefix(ip).await {
            Ok(prefix) => prefix,
            Err(_) => None,
        }
    }

    /// Fetch BGP prefix from external APIs and store in database
    async fn fetch_and_store_prefix(&self, ip: std::net::IpAddr) -> anyhow::Result<Option<BgpPrefix>> {
        debug!("Fetching BGP data from external APIs for {}", ip);

        // Try BGPView API first
        if let Some(prefix) = self.fetch_from_bgpview(ip).await? {
            self.store_prefix(&prefix).await?;
            return Ok(Some(prefix));
        }

        // Fallback to RIPE Stat API
        if let Some(prefix) = self.fetch_from_ripe_stat(ip).await? {
            self.store_prefix(&prefix).await?;
            return Ok(Some(prefix));
        }

        Ok(None)
    }

    /// Fetch BGP data from BGPView API
    async fn fetch_from_bgpview(&self, ip: std::net::IpAddr) -> anyhow::Result<Option<BgpPrefix>> {
        let url = format!("https://api.bgpview.io/ip/{}", ip);

        let response = match self.client.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => resp,
            Ok(resp) => {
                debug!("BGPView API returned status: {}", resp.status());
                return Ok(None);
            }
            Err(e) => {
                warn!("BGPView API request failed: {}", e);
                return Ok(None);
            }
        };

        let json: serde_json::Value = response.json().await.map_err(|e| anyhow::anyhow!("JSON parse error: {}", e))?;

        if let Some(data) = json.get("data") {
            if let Some(prefixes) = data.get("prefixes").and_then(|p| p.as_array()) {
                if let Some(prefix_data) = prefixes.first() {
                    let prefix_str = prefix_data.get("prefix")
                        .and_then(|p| p.as_str())
                        .ok_or_else(|| anyhow::anyhow!("Missing prefix in BGPView response"))?;

                    let prefix: IpNet = prefix_str.parse()?;

                    let asn_data = prefix_data.get("asn")
                        .ok_or_else(|| anyhow::anyhow!("Missing ASN data in BGPView response"))?;

                    let asn = asn_data.get("asn")
                        .and_then(|a| a.as_u64())
                        .ok_or_else(|| anyhow::anyhow!("Invalid ASN in BGPView response"))? as u32;

                    let description = asn_data.get("description")
                        .and_then(|d| d.as_str())
                        .unwrap_or("")
                        .to_string();

                    let country_code = asn_data.get("country_code")
                        .and_then(|c| c.as_str())
                        .unwrap_or("")
                        .to_string();

                    return Ok(Some(BgpPrefix {
                        prefix,
                        asn,
                        description,
                        country_code,
                        registry: "bgpview".to_string(),
                        last_updated: chrono::Utc::now(),
                    }));
                }
            }
        }

        Ok(None)
    }

    /// Fetch BGP data from RIPE Stat API
    async fn fetch_from_ripe_stat(&self, ip: std::net::IpAddr) -> anyhow::Result<Option<BgpPrefix>> {
        let url = format!("https://stat.ripe.net/data/network-info/data.json?resource={}", ip);

        let response = match self.client.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => resp,
            Ok(resp) => {
                debug!("RIPE Stat API returned status: {}", resp.status());
                return Ok(None);
            }
            Err(e) => {
                warn!("RIPE Stat API request failed: {}", e);
                return Ok(None);
            }
        };

        let json: serde_json::Value = response.json().await.map_err(|e| anyhow::anyhow!("JSON parse error: {}", e))?;

        if let Some(data) = json.get("data") {
            let prefix_str = data.get("prefix")
                .and_then(|p| p.as_str())
                .ok_or_else(|| anyhow::anyhow!("Missing prefix in RIPE response"))?;

            let prefix: IpNet = prefix_str.parse()?;

            let asn = data.get("asns")
                .and_then(|a| a.as_array())
                .and_then(|arr| arr.first())
                .and_then(|asn| asn.as_u64())
                .unwrap_or(0) as u32;

            return Ok(Some(BgpPrefix {
                prefix,
                asn,
                description: String::new(), // RIPE Stat doesn't always provide description
                country_code: String::new(),
                registry: "ripe".to_string(),
                last_updated: chrono::Utc::now(),
            }));
        }

        Ok(None)
    }

    /// Store BGP prefix in database
    async fn store_prefix(&self, prefix: &BgpPrefix) -> anyhow::Result<()> {
        match self.db_type {
            DatabaseType::Sqlite => {
                sqlx::query(
                    r#"INSERT OR REPLACE INTO bgp_prefixes
                    (prefix, asn, description, country_code, registry, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?)"#
                )
                    .bind(prefix.prefix.to_string())
                    .bind(prefix.asn as i64)
                    .bind(&prefix.description)
                    .bind(&prefix.country_code)
                    .bind(&prefix.registry)
                    .bind(prefix.last_updated.to_rfc3339())
                    .execute(&self.pool)
                    .await?;
            }
            DatabaseType::Postgres => {
                let query_str = format!(
                    r#"INSERT INTO bgp_prefixes
                    (prefix, asn, description, country_code, registry, last_updated)
                    VALUES ($1, $2, $3, $4, $5, '{}')
                    ON CONFLICT (prefix) DO UPDATE SET
                        asn = EXCLUDED.asn,
                        description = EXCLUDED.description,
                        country_code = EXCLUDED.country_code,
                        registry = EXCLUDED.registry,
                        last_updated = EXCLUDED.last_updated"#,
                    prefix.last_updated.to_rfc3339()
                );

                sqlx::query(&query_str)
                    .bind(prefix.prefix.to_string())
                    .bind(prefix.asn as i64)
                    .bind(&prefix.description)
                    .bind(&prefix.country_code)
                    .bind(&prefix.registry)
                    .execute(&self.pool)
                    .await?;
            }
        }

        debug!("Stored BGP prefix {} in database", prefix.prefix);
        Ok(())
    }

    /// Bulk update BGP data from RIR delegated statistics
    async fn update_bgp_data(&self) -> anyhow::Result<()> {
        info!("Starting full BGP data update from RIR sources");

        let mut _prefixes = Vec::new();

        // Fetch from all RIR sources
        for (registry, url) in &[
            ("ripencc", "https://ftp.ripe.net/pub/stats/ripencc/delegated-ripencc-extended-latest"),
            ("arin", "https://ftp.arin.net/pub/stats/arin/delegated-arin-extended-latest"),
            ("apnic", "https://ftp.apnic.net/pub/stats/apnic/delegated-apnic-extended-latest"),
            ("lacnic", "https://ftp.lacnic.net/pub/stats/lacnic/delegated-lacnic-extended-latest"),
            ("afrinic", "https://ftp.afrinic.net/pub/stats/afrinic/delegated-afrinic-extended-latest"),
        ] {
            match self.fetch_delegated_stats(registry, url).await {
                Ok(mut prefixes) => {
                    info!("Fetched {} prefixes from {}", prefixes.len(), registry);
                    _prefixes.append(&mut prefixes);
                }
                Err(e) => warn!("Failed to fetch data from {}: {}", registry, e),
            }
        }

        // Fetch RouteViews ASN data for better ASN coverage
        match self.fetch_routeviews_asn_data().await {
            Ok(asn_map) => {
                info!("Fetched {} ASN mappings from RouteViews", asn_map.len());
                self.enrich_prefixes(&mut _prefixes, &asn_map);
            }
            Err(e) => warn!("Failed to fetch RouteViews data: {}", e),
        }

        info!("Total prefixes collected: {}", _prefixes.len());

        // Bulk insert into database
        self.insert_prefixes(_prefixes).await?;

        info!("BGP data update completed successfully");
        Ok(())
    }

    /// Fetch and parse RIR delegated statistics
    async fn fetch_delegated_stats(&self, registry: &str, url: &str) -> anyhow::Result<Vec<BgpPrefix>> {
        debug!("Fetching delegated stats from {}: {}", registry, url);

        let response = self.client.get(url).send().await?;
        let content = response.text().await?;

        self.parse_delegated_stats(registry, &content).await
    }

    /// Parse RIR delegated statistics format
    async fn parse_delegated_stats(&self, _registry: &str, content: &str) -> anyhow::Result<Vec<BgpPrefix>> {
        let mut prefixes = Vec::new();
        let mut line_count = 0;

        for line in content.lines() {
            line_count += 1;

            // Skip comments and metadata
            if line.starts_with('#') || line.is_empty() || line_count <= 5 {
                continue;
            }

            let parts: Vec<&str> = line.split('|').collect();
            if parts.len() < 7 {
                continue;
            }

            let reg = parts[0];
            let cc = parts[1];
            let type_field = parts[2];
            let start = parts[3];
            let value = parts[4];
            let status = parts[6];

            // Only process assigned and allocated resources
            if status != "assigned" && status != "allocated" {
                continue;
            }

            match type_field {
                "ipv4" => {
                    if let (Ok(start_ip), Ok(count)) = (
                        start.parse::<std::net::Ipv4Addr>(),
                        value.parse::<u32>()
                    ) {
                        // Calculate prefix length from IP count
                        if count > 0 && (count & (count - 1)) == 0 { // Power of 2
                            let prefix_len = 32 - count.trailing_zeros() as u8;
                            if let Ok(cidr) = format!("{}/{}", start_ip, prefix_len).parse::<IpNet>() {
                                prefixes.push(BgpPrefix {
                                    prefix: cidr,
                                    asn: 0, // Will be filled later from RouteViews data
                                    description: format!("{} allocation", cc.to_uppercase()),
                                    country_code: cc.to_string(),
                                    registry: reg.to_string(),
                                    last_updated: chrono::Utc::now(),
                                });
                            }
                        }
                    }
                }
                "ipv6" => {
                    if let Ok(prefix_len) = value.parse::<u8>() {
                        if let Ok(cidr) = format!("{}/{}", start, prefix_len).parse::<IpNet>() {
                            prefixes.push(BgpPrefix {
                                prefix: cidr,
                                asn: 0,
                                description: format!("{} allocation", cc.to_uppercase()),
                                country_code: cc.to_string(),
                                registry: reg.to_string(),
                                last_updated: chrono::Utc::now(),
                            });
                        }
                    }
                }
                _ => continue,
            }
        }

        Ok(prefixes)
    }

    /// Fetch ASN to prefix mappings from RouteViews
    async fn fetch_routeviews_asn_data(&self) -> anyhow::Result<std::collections::HashMap<IpNet, u32>> {
        let url = "http://archive.routeviews.org/dnszones/originas.bz2";

        let response = self.client.get(url).send().await?;
        let compressed_data = response.bytes().await?;

        // Decompress bz2 data
        let cursor = std::io::Cursor::new(compressed_data);
        let mut decoder = BzDecoder::new(cursor);
        let mut content = String::new();
        std::io::Read::read_to_string(&mut decoder, &mut content)?;

        let mut asn_map = std::collections::HashMap::new();

        // Parse originas format: "prefix origin_asn"
        for line in content.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                if let (Ok(prefix), Ok(asn)) = (
                    parts[0].parse::<IpNet>(),
                    parts[1].parse::<u32>()
                ) {
                    asn_map.insert(prefix, asn);
                }
            }
        }

        Ok(asn_map)
    }

    /// Enrich prefix data with ASN information from RouteViews
    fn enrich_prefixes(
        &self,
        prefixes: &mut Vec<BgpPrefix>,
        asn_map: &std::collections::HashMap<IpNet, u32>
    ) {
        for prefix in prefixes.iter_mut() {
            if prefix.asn == 0 {
                // Look for exact match or containing prefix
                if let Some(&asn) = asn_map.get(&prefix.prefix) {
                    prefix.asn = asn;
                } else {
                    // Find most specific containing prefix
                    let mut best_asn = 0;
                    let mut best_len = 0u8;

                    for (net, &asn) in asn_map {
                        if net.contains(&prefix.prefix.network()) {
                            let len = match net {
                                IpNet::V4(n) => n.prefix_len(),
                                IpNet::V6(n) => n.prefix_len(),
                            };
                            if len > best_len {
                                best_len = len;
                                best_asn = asn;
                            }
                        }
                    }

                    if best_asn != 0 {
                        prefix.asn = best_asn;
                    }
                }
            }
        }
    }

    /// Bulk insert prefixes into database with transaction
    async fn insert_prefixes(&self, prefixes: Vec<BgpPrefix>) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;

        // Clean up old data (older than 30 days)
        let cleanup_query = match self.db_type {
            DatabaseType::Sqlite => "DELETE FROM bgp_prefixes WHERE last_updated < datetime('now', '-30 days')",
            DatabaseType::Postgres => "DELETE FROM bgp_prefixes WHERE last_updated < NOW() - INTERVAL '30 days'",
        };
        sqlx::query(cleanup_query)
            .execute(&mut *tx)
            .await?;

        // Insert new data in batches
        const BATCH_SIZE: usize = 1000;
        let total_batches = (prefixes.len() + BATCH_SIZE - 1) / BATCH_SIZE;

        for (batch_idx, chunk) in prefixes.chunks(BATCH_SIZE).enumerate() {
            info!("Inserting batch {}/{} ({} prefixes)", batch_idx + 1, total_batches, chunk.len());

            match self.db_type {
                DatabaseType::Sqlite => {
                    for prefix in chunk {
                        sqlx::query(
                            r#"INSERT OR REPLACE INTO bgp_prefixes
                            (prefix, asn, description, country_code, registry, last_updated)
                            VALUES (?, ?, ?, ?, ?, ?)"#
                        )
                            .bind(prefix.prefix.to_string())
                            .bind(prefix.asn as i64)
                            .bind(&prefix.description)
                            .bind(&prefix.country_code)
                            .bind(&prefix.registry)
                            .bind(prefix.last_updated.to_rfc3339())
                            .execute(&mut *tx)
                            .await?;
                    }
                }
                DatabaseType::Postgres => {
                    for prefix in chunk {
                        let query_str = format!(
                            r#"INSERT INTO bgp_prefixes
                            (prefix, asn, description, country_code, registry, last_updated)
                            VALUES ($1, $2, $3, $4, $5, '{}')
                            ON CONFLICT (prefix) DO UPDATE SET
                                asn = EXCLUDED.asn,
                                description = EXCLUDED.description,
                                country_code = EXCLUDED.country_code,
                                registry = EXCLUDED.registry,
                                last_updated = EXCLUDED.last_updated"#,
                            prefix.last_updated.to_rfc3339()
                        );

                        sqlx::query(&query_str)
                            .bind(prefix.prefix.to_string())
                            .bind(prefix.asn as i64)
                            .bind(&prefix.description)
                            .bind(&prefix.country_code)
                            .bind(&prefix.registry)
                            .execute(&mut *tx)
                            .await?;
                    }
                }
            }
        }

        tx.commit().await?;
        info!("Successfully inserted {} prefixes into database", prefixes.len());

        Ok(())
    }

    /// Get cache statistics for monitoring
    fn get_cache_stats(&self) -> (u64, u64) {
        (self.lookup_cache.entry_count(), self.lookup_cache.weighted_size())
    }
}

#[derive(Clone)]
struct Dbs {
    country: Option<Arc<Reader<Vec<u8>>>>,
    asn: Option<Arc<Reader<Vec<u8>>>>,
}

#[derive(Clone)]
struct PrefixEntry {
    net: IpNet,
    resp: LookupResponse,
    prefix_len: u8,
}

#[derive(Clone)]
struct Shared {
    dbs: Arc<RwLock<Dbs>>,
    ip_cache: Cache<String, LookupResponse>,
    prefix_cache: Arc<RwLock<Vec<PrefixEntry>>>,
    bgp_storage: BgpStorage,
    token: Option<String>,
}

impl Shared {
    async fn new(dbs: Dbs, ttl: Duration, token: Option<String>, database_path: &str) -> anyhow::Result<Self> {
        Ok(Self {
            dbs: Arc::new(RwLock::new(dbs)),
            ip_cache: Cache::builder().time_to_live(ttl).build(),
            prefix_cache: Arc::new(RwLock::new(Vec::new())),
            bgp_storage: BgpStorage::new(database_path).await?,
            token,
        })
    }

    fn check_auth(&self, md: &MetadataMap) -> Result<(), Status> {
        if let Some(t) = &self.token {
            match md.get("authorization").and_then(|v| v.to_str().ok()) {
                Some(s) if s.starts_with("Bearer ") && &s["Bearer ".len()..] == t => Ok(()),
                _ => Err(Status::unauthenticated("invalid authorization token")),
            }
        } else {
            Ok(())
        }
    }
}

/// Logging interceptor for access logs
#[derive(Clone)]
struct LoggingInterceptor;

impl Interceptor for LoggingInterceptor {
    fn call(&mut self, request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        let start_time = std::time::Instant::now();

        // Extract metadata for logging
        let method = request
            .metadata()
            .get(":path")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("unknown");

        let remote_addr = request
            .remote_addr()
            .map(|addr| addr.to_string())
            .unwrap_or_else(|| "unknown".to_string());

        let user_agent = request
            .metadata()
            .get("user-agent")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("unknown");

        // Log the request
        info!(
            remote_addr = %remote_addr,
            method = %method,
            user_agent = %user_agent,
            "Incoming gRPC request"
        );

        Ok(request)
    }
}

/// Response logging middleware
fn log_response(method: &str, remote_addr: &str, status: &Status, duration: Duration) {
    info!(remote_addr = %remote_addr, method = %method, status_code = %status.code(), duration_ms = %duration.as_millis(), "gRPC response");
}

#[derive(Clone)]
struct IpToAsnSvc {
    shared: Shared,
}

#[tonic::async_trait]
impl IpToAsnService for IpToAsnSvc {
    async fn lookup(&self, req: Request<LookupRequest>) -> Result<Response<LookupResponse>, Status> {
        let start_time = std::time::Instant::now();
        let remote_addr = req.remote_addr().map(|a| a.to_string()).unwrap_or_else(|| "unknown".to_string());

        // Check authentication
        if let Err(status) = self.shared.check_auth(req.metadata()) {
            log_response("lookup", &remote_addr, &status, start_time.elapsed());
            return Err(status);
        }

        let request_data = req.into_inner();
        let ip = request_data.ip_address;

        if ip.is_empty() {
            let status = Status::invalid_argument("ip_address is required and cannot be empty");
            log_response("lookup", &remote_addr, &status, start_time.elapsed());
            return Err(Status::invalid_argument("ip_address required"));
        }

        // Validate IP address format
        let stdip = match ip.parse::<std::net::IpAddr>() {
            Ok(addr) => addr,
            Err(e) => {
                let status = Status::invalid_argument(format!("Invalid IP address format: {}", e));
                log_response("lookup", &remote_addr, &status, start_time.elapsed());
                return Err(status);
            }
        };

        // Check IP cache first
        match self.shared.ip_cache.get(&ip).await {
            Some(cached_response) => {
                debug!("Cache hit for IP: {}", ip);
                let status = Status::ok("");
                log_response("lookup", &remote_addr, &status, start_time.elapsed());
                return Ok(Response::new(cached_response));
            }
            None => {
                debug!("Cache miss for IP: {}", ip);
            }
        }

        let cached_prefix_result = {
            let pc = self.shared.prefix_cache.read().await;
            let mut best: Option<(u8, LookupResponse)> = None;

            for e in pc.iter() {
                if e.net.contains(&stdip) {
                    if best.as_ref().map(|(l, _)| *l).unwrap_or(0) < e.prefix_len {
                        best = Some((e.prefix_len, e.resp.clone()));
                    }
                }
            }

            best.map(|(_, resp)| resp)
        };

        if let Some(cached_resp) = cached_prefix_result {
            self.shared.ip_cache.insert(ip.clone(), cached_resp.clone()).await;
            let status = Status::ok("");
            log_response("lookup", &remote_addr, &status, start_time.elapsed());
            return Ok(Response::new(cached_resp));
        }

        // Initialize response
        let mut resp = LookupResponse {
            announced: false,
            as_number: 0,
            cidr: Vec::new(),
            country_code: String::new(),
            description: String::new(),
        };

        // Get data from GeoIP2 databases
        let dbs = self.shared.dbs.read().await;

        // Country lookup
        if let Some(db) = &dbs.country {
            match db.lookup::<geoip2::Country>(stdip) {
                Ok(country_rec) => {
                    if let Some(country_data) = country_rec {
                        if let Some(c) = country_data.country {
                            if let Some(code) = c.iso_code {
                                resp.country_code = code.to_string();
                                debug!("Found country code for {}: {}", ip, resp.country_code);
                            }
                        }
                    }
                }
                Err(e) => {
                    debug!("GeoIP2 Country lookup failed for {}: {}", ip, e);
                }
            }
        } else {
            debug!("GeoIP2 Country database not available");
        }

        // ASN lookup
        if let Some(db) = &dbs.asn {
            match db.lookup::<geoip2::Asn>(stdip) {
                Ok(asn_rec) => {
                    if let Some(asn_data) = asn_rec {
                        if let Some(asn) = asn_data.autonomous_system_number {
                            resp.as_number = asn;
                            resp.announced = asn != 0;
                            debug!("Found ASN for {}: {}", ip, asn);
                        }
                        if let Some(org) = asn_data.autonomous_system_organization {
                            resp.description = org.to_string();
                        }
                    }
                }
                Err(e) => {
                    debug!("GeoIP2 ASN lookup failed for {}: {}", ip, e);
                }
            }
        } else {
            debug!("GeoIP2 ASN database not available");
        }

        // Try to get BGP prefix information (this may trigger external API calls)
        let bgp_result = self.shared.bgp_storage.find_prefix_simple(stdip).await;
        match bgp_result {
            Some(ref bgp_prefix) => {
                debug!("Found BGP prefix for {}: {}", ip, bgp_prefix.prefix);
                resp.cidr.push(bgp_prefix.prefix.to_string());

                // Use BGP data if GeoIP2 data is missing
                if resp.as_number == 0 {
                    resp.as_number = bgp_prefix.asn;
                    resp.announced = bgp_prefix.asn != 0;
                }

                if resp.description.is_empty() && !bgp_prefix.description.is_empty() {
                    resp.description = bgp_prefix.description.clone();
                }

                if resp.country_code.is_empty() && !bgp_prefix.country_code.is_empty() {
                    resp.country_code = bgp_prefix.country_code.clone();
                }

                // Cache the prefix for future lookups
                let prefix_len = match bgp_prefix.prefix {
                    IpNet::V4(n) => n.prefix_len(),
                    IpNet::V6(n) => n.prefix_len(),
                };

                let cache_entry = PrefixEntry {
                    net: bgp_prefix.prefix,
                    resp: resp.clone(),
                    prefix_len,
                };

                // Cache management
                match self.shared.prefix_cache.try_write() {
                    Ok(mut pc) => {
                        if !pc.iter().any(|e| e.net == cache_entry.net) {
                            pc.push(cache_entry);

                            // Limit prefix cache size
                            if pc.len() > 1000 {
                                pc.sort_by(|a, b| b.prefix_len.cmp(&a.prefix_len));
                                pc.truncate(800);
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to acquire prefix cache write lock: {}", e);
                    }
                }
            }
            None => {
                debug!("No BGP prefix found for {} after external lookup", ip);
            }
        }

        // Cache the final result
        self.shared.ip_cache.insert(ip.clone(), resp.clone()).await;

        let status = Status::ok("");
        log_response("lookup", &remote_addr, &status, start_time.elapsed());
        Ok(Response::new(resp))
    }
}

#[derive(Clone)]
struct ReputeSvc {
    shared: Shared,
}

#[tonic::async_trait]
impl ReputeService for ReputeSvc {
    async fn query(&self, req: Request<ReputeServiceQueryRequest>) -> Result<Response<ReputeServiceQueryResponse>, Status> {
        let start_time = std::time::Instant::now();
        let remote_addr = req.remote_addr().map(|a| a.to_string()).unwrap_or_else(|| "unknown".to_string());

        // Check authentication
        if let Err(status) = self.shared.check_auth(req.metadata()) {
            log_response("reputation_query", &remote_addr, &status, start_time.elapsed());
            return Err(status);
        }

        // Extract metadata before consuming req
        let metadata = req.metadata().clone();
        let req_in = req.into_inner();
        let ip = req_in.ip_address;

        if ip.is_empty() {
            let status = Status::invalid_argument("ip_address is required and cannot be empty");
            log_response("reputation_query", &remote_addr, &status, start_time.elapsed());
            return Err(Status::invalid_argument("ip_address required"));
        }

        // Reuse the IP-to-ASN lookup logic
        let lookup_req = LookupRequest { ip_address: ip.clone() };
        let mut lookup_request = Request::new(lookup_req);

        // Copy metadata (including authorization)
        *lookup_request.metadata_mut() = metadata;

        let ip_svc = IpToAsnSvc { shared: self.shared.clone() };
        let lookup_resp = match ip_svc.lookup(lookup_request).await {
            Ok(response) => response.into_inner(),
            Err(status) => {
                error!("Failed to lookup ASN info for reputation query on IP {}: {}", ip, status);
                log_response("reputation_query", &remote_addr, &status, start_time.elapsed());
                return Err(status);
            }
        };

        let rep = ReputeServiceQueryResponse {
            has_match: false,
            ip_lists: Vec::<IpList>::new(),
            suggested_challenge: String::new(),
            asn_info: Some(lookup_resp),
        };

        let status = Status::ok("");
        log_response("reputation_query", &remote_addr, &status, start_time.elapsed());
        Ok(Response::new(rep))
    }
}

/// Reload GeoIP2 databases from disk
async fn reload_mmdbs(shared: &Shared, db_dir: &PathBuf) -> anyhow::Result<()> {
    let country_path = db_dir.join("GeoLite2-Country.mmdb");
    let asn_path = db_dir.join("GeoLite2-ASN.mmdb");

    let new_country = if country_path.exists() {
        Some(Arc::new(Reader::open_readfile(&country_path)?))
    } else {
        None
    };

    let new_asn = if asn_path.exists() {
        Some(Arc::new(Reader::open_readfile(&asn_path)?))
    } else {
        None
    };

    // Update databases
    let mut dbs = shared.dbs.write().await;
    dbs.country = new_country;
    dbs.asn = new_asn;
    drop(dbs);

    // Clear caches to ensure fresh data
    shared.prefix_cache.write().await.clear();
    shared.ip_cache.invalidate_all();

    info!("GeoIP2 databases reloaded successfully");
    Ok(())
}

/// Setup cron scheduler for BGP data updates
async fn cron(bgp_storage: BgpStorage) -> anyhow::Result<JobScheduler> {
    let scheduler = JobScheduler::new().await?;

    // Daily BGP update at 02:00 UTC
    scheduler.add(
        Job::new_async("0 0 2 * * *", move |_uuid, _l| {
            let storage = bgp_storage.clone();
            Box::pin(async move {
                info!("Starting scheduled BGP data update");
                match storage.update_bgp_data().await {
                    Ok(()) => info!("Scheduled BGP update completed successfully"),
                    Err(e) => error!("Scheduled BGP update failed: {}", e),
                }
            })
        })?
    ).await?;

    scheduler.start().await?;
    info!("BGP update scheduler started (daily at 02:00 UTC)");

    Ok(scheduler)
}

/// Setup signal handlers for graceful shutdown and reloads
async fn setup_signal_handlers(shared: Shared, db_dir: PathBuf) {
    // SIGHUP handler for manual reloads
    let shared_sighup = shared.clone();
    let db_dir_sighup = db_dir.clone();

    tokio::spawn(async move {
        use tokio::signal::unix::{signal, SignalKind};

        let mut stream = match signal(SignalKind::hangup()) {
            Ok(s) => s,
            Err(e) => {
                warn!("Could not bind SIGHUP handler: {}", e);
                return;
            }
        };

        while stream.recv().await.is_some() {
            info!("SIGHUP received: reloading databases and triggering BGP update");

            // Reload GeoIP2 databases
            if let Err(e) = reload_mmdbs(&shared_sighup, &db_dir_sighup).await {
                error!("GeoIP2 database reload failed: {}", e);
            }

            // Trigger BGP data update
            if let Err(e) = shared_sighup.bgp_storage.update_bgp_data().await {
                error!("BGP data update failed: {}", e);
            } else {
                info!("Manual BGP data update completed successfully");
            }
        }
    });

    // SIGUSR1 handler for cache statistics
    let shared_stats = shared.clone();
    tokio::spawn(async move {
        use tokio::signal::unix::{signal, SignalKind};

        let mut stream = match signal(SignalKind::user_defined1()) {
            Ok(s) => s,
            Err(e) => {
                warn!("Could not bind SIGUSR1 handler: {}", e);
                return;
            }
        };

        while stream.recv().await.is_some() {
            let ip_cache_stats = (
                shared_stats.ip_cache.entry_count(),
                shared_stats.ip_cache.weighted_size()
            );
            let prefix_cache_len = shared_stats.prefix_cache.read().await.len();
            let bgp_cache_stats = shared_stats.bgp_storage.get_cache_stats();

            info!("Cache Statistics:");
            info!("  IP Cache: {} entries, {} bytes", ip_cache_stats.0, ip_cache_stats.1);
            info!("  Prefix Cache: {} entries", prefix_cache_len);
            info!("  BGP Cache: {} entries, {} bytes", bgp_cache_stats.0, bgp_cache_stats.1);
        }
    });
}

/// Global error handler for unhandled errors
fn setup_panic_handler() {
    std::panic::set_hook(Box::new(|panic_info| {
        let location = panic_info.location().unwrap_or_else(|| {
            std::panic::Location::caller()
        });

        let message = if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = panic_info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "Unknown panic".to_string()
        };

        error!(
            panic.file = %location.file(),
            panic.line = %location.line(),
            panic.column = %location.column(),
            panic.message = %message,
            "Application panic occurred"
        );
    }));
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // force compiler to include drivers
    sqlx::any::install_default_drivers();

    // Initialize logging
    tracing_subscriber::fmt::init();

    // Setup panic handler for better error reporting
    setup_panic_handler();

    // Load configuration from environment
    let db_dir = env::var("GEOIP_DB_DIR").unwrap_or_else(|_| "/usr/share/GeoIP".to_string());
    let port: u16 = env::var("PORT").ok().and_then(|s| s.parse().ok()).unwrap_or(50051);
    let token = env::var("TOKEN").ok();
    let cache_ttl_secs: u64 = env::var("CACHE_TTL_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(600);
    let database_uri = env::var("DATABASE_URI")
        .unwrap_or_else(|_| "sqlite:bgp_data.db".to_string());

    info!("Starting GeoIP service with configuration:");
    info!("  GeoIP2 DB directory: {}", db_dir);
    info!("  Database URI: {}", database_uri);
    info!("  Port: {}", port);
    info!("  Cache TTL: {} seconds", cache_ttl_secs);
    info!("  Authentication: {}", if token.is_some() { "enabled" } else { "disabled" });

    // Initialize GeoIP2 databases
    let db_dir_path = PathBuf::from(&db_dir);
    let country_path = db_dir_path.join("GeoLite2-Country.mmdb");
    let asn_path = db_dir_path.join("GeoLite2-ASN.mmdb");

    let country = if country_path.exists() {
        info!("Loading GeoIP2 Country database: {:?}", country_path);
        Some(Arc::new(Reader::open_readfile(&country_path)
            .with_context(|| format!("Failed to open country database: {:?}", country_path))?))
    } else {
        warn!("GeoIP2 Country database not found: {:?}", country_path);
        None
    };

    let asn = if asn_path.exists() {
        info!("Loading GeoIP2 ASN database: {:?}", asn_path);
        Some(Arc::new(Reader::open_readfile(&asn_path)
            .with_context(|| format!("Failed to open ASN database: {:?}", asn_path))?))
    } else {
        warn!("GeoIP2 ASN database not found: {:?}", asn_path);
        None
    };

    // Initialize shared state
    let shared = Shared::new(
        Dbs { country, asn },
        Duration::from_secs(cache_ttl_secs),
        token,
        &database_uri
    ).await.context("Failed to initialize shared state")?;

    // Setup BGP data scheduler
    let _scheduler = cron(shared.bgp_storage.clone()).await
        .context("Failed to setup BGP scheduler")?;

    // Setup signal handlers
    setup_signal_handlers(shared.clone(), db_dir_path).await;

    // Create gRPC services
    let ip_svc = IpToAsnSvc { shared: shared.clone() };
    let rep_svc = ReputeSvc { shared: shared.clone() };

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("Starting gRPC server on {}", addr);

    static FILE_DESCRIPTOR_SET: &'static [u8] = include_bytes!("../proto/descriptor.bin");

    let reflection_service = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();

    // Start the server with graceful shutdown
    Server::builder()
        .layer(tower::ServiceBuilder::new().layer(tonic::service::interceptor(LoggingInterceptor)))
        .add_service(
            IpToAsnServiceServer::new(ip_svc)
                .accept_compressed(CompressionEncoding::Gzip)
        )
        .add_service(
            ReputeServiceServer::new(rep_svc)
                .accept_compressed(CompressionEncoding::Gzip)
        )
        .add_service(reflection_service)
        .serve_with_shutdown(addr, async {
            signal::ctrl_c().await.expect("Failed to listen for ctrl-c");
            info!("Shutdown signal received, stopping server gracefully...");
        })
        .await
        .context("gRPC server failed")?;

    info!("Server stopped");
    Ok(())
}
