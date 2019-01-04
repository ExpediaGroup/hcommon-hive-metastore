# [TBD]
### Changed
* Refactored project to remove checkstyle and findbugs warnings, which does not impact functionality.
* Upgraded `hotels-oss-parent` to 2.3.5 (was 2.3.3).

# [1.2.3] - 2018-10-29
### Fixed
* Renamed function that indicates if strict host key checking is enabled to avoid causing a Spring Bean creation error.

# [1.2.2] - 2018-10-23
### Added
* Boolean function to indicate whether strict host key checking is enabled for MetastoreTunnel.

# [1.2.1] - 2018-10-15
### Changed
* Upgraded hotels-oss-parent to 2.3.3 (was 2.1.0).

### Added
* PartitionIterator can optionally traverse in reverse order.
* MetastoreUnavailableException and MetaStoreUriNormaliser implementations.

# [1.2.0] - 2018-09-21
### Added
* MetastoreTunnel as a common class.

# [1.1.0] - 2018-06-15
### Added
* ConditionalMetaStoreClientFactory implementations.

# [1.0.0] - 2018-06-04
### Added
* Initial general purpose Hive metastore code.
