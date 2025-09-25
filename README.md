This software is a gRPC wrapper over the GeoIP2-ASN database, which also uses a local BGP route database. It implements the basic functionality of the Thoth service used for [Anubis](https://anubis.techaro.lol/), but is not a complete replacement for it. For full functionality, consider [becoming a sponsor](https://github.com/sponsors/Xe) for Xe and having access to the real Thoth.

!!! This software was partially written using prompt engineering as a local experiment and is not recommended for use in production environments. !!!

Supported environment variables:

* GEOIP_DB_DIR - path to a folder containing GeoLite2-ASN.mmdb file
* PORT - gRPC service port (default is 50051)
* DATABASE_URI - database url (either postgres://user:password@host:port/dbname or sqlite:/path/to/database/file.db)
* TOKEN - a secret used to Bearer auth
* METRICS_PORT - prometheus metrics port



