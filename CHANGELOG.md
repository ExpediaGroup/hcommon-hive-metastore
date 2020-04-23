# [1.4.2] - TBD
### Changed
* Upgraded version of `hive.version` to `2.3.7` (was `2.3.4`). Allows hcommon-hive-metastore to be used on JDK>=9.

# [1.4.1] - 2019-10-03
### Fixed
* Made `newTablePathResolver` method in `TablePathResolver` public.

# [1.4.0] - 2019-09-30
### Added
* Classes for resolving partition locations and table location from a metastore.

# [1.3.0] - 2019-02-27
### Fixed
* Added Hive 1.2 compatible `tableExists` method. See [#115](https://github.com/HotelsDotCom/circus-train/issues/115).

# [1.2.4] - 2019-01-10
### Changed
* Refactored project to remove checkstyle and findbugs warnings, which does not impact functionality.
* Upgraded `hotels-oss-parent` to 2.3.5 (was 2.3.3).
* Upgraded Hive version to 2.3.4 (was 2.3.2).
* Removed transitive (provided) dependency on `hbase-client`.
### Fixed
* Removed 'isOpen()' method from CloseableMetaStoreClient. See [#11](https://github.com/HotelsDotCom/hcommon-hive-metastore/issues/11).
* Issue where the wrong exception was being propagated in the compatibility layer. See [#16](https://github.com/HotelsDotCom/hcommon-hive-metastore/issues/16).

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
