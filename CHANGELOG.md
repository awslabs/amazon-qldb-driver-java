# Changelog All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic
Versioning](https://semver.org/spec/v2.0.0.html).

# [2.0.0-preview](https://github.com/awslabs/amazon-qldb-driver-java/releases/tag/v2.0.0-rc.1) - 2020-06-19 

We are adding new changes to the QLDB Driver for Java. However Java driver 1.x will
still be maintained and supported until further notice. We recommend to move to the version
2.0 as it improves the performance and transaction management.

### :tada:Added 

* Added the `getTableNames` method to the `QldbDriver` class. For more details please
read the [release
notes](https://github.com/awslabs/amazon-qldb-driver-java/releases/tag/v2.0.0-rc.1).

### :hammer_and_wrench: Improvements

* Session pooling functionality moved to `QldbDriver`.  For more details please
read the [release
notes](https://github.com/awslabs/amazon-qldb-driver-java/releases/tag/v2.0.0-rc.1).
* Improved the performance of the driver by removing unnecessary calls to QLDB. 
* Upgraded dependency from the AWS SDK for Java v1 to the AWS SDK for Java v2. Note,
the current driver version v1.x is still supported and this version doesn't
deprecate it. For more details please read the [release
notes](https://github.com/awslabs/amazon-qldb-driver-java/releases/tag/v2.0.0-rc.1).
 

### :bug: Fixed 
* Fixed the issue where two different Ion Java packages were present in the classpath.

### :rotating_light:Removed

* `PooledQldbDriver` has been removed. Please use `QldbDriver` instead. For more
details please read the [release
notes](https://github.com/awslabs/amazon-qldb-driver-java/releases/tag/v2.0.0-rc.1).

* `QldbSession` and `Transaction` classes have been removed.  Please use
`QldbDriver` instead. For more details please read the [release
notes](https://github.com/awslabs/amazon-qldb-driver-java/releases/tag/v2.0.0-rc.1).

* `QldbDriver.execute(String)` method has been removed as we found it made the 
driver confusing about when to use one execute method over the other. Please use `QldbDriver
.execute(ExecutorNoReturn executor)` method  or `QldbDriver.execute(Executor executor)` instead.

## [1.1.0](https://github.com/awslabs/amazon-qldb-driver-java/compare/v1.0.2...v1.1.0) - 2020-03-20 
### Features 
- Add the execute method to the `PooledQldbDriver`. 
- Use varargs on the execute methods to pass Ion parameters.

## [1.0.2](https://github.com/awslabs/amazon-qldb-driver-java/compare/v1.0.1...v1.0.2) - 2019-12-17 

### Bug Fixes 
- Bump version of the AWS SDK to 1.11.649 
- Fix an issue that will make the driver throw an `InvalidSessionException` when
executing a transaction. In the initial release of the driver, if a session
becomes invalid while using the `PooledQldbSession`'s `execute` convenience
methods, then the transaction will fail completely for the caller, as the
`InvalidSessionException` is re-thrown. To prevent the caller from needing to
write additional retry logic, the `execute` methods will now transparently
replace an invalid session with a new one and retry the transaction but still be
limited to the number of retries configured.

### [1.0.1](https://github.com/awslabs/amazon-qldb-driver-java/releases/tag/v1.0.1) - 2019-10-31 

### Features 
- Provides a layer on top of the AWS SDK for Java called `driver` that simplifies how to execute transactions with QLDB.
- Includes a Pooled driver that manages the sessions. 
- Provides an execute method that starts a session, sends PartiQL statements to QLDB, and commits the transaction 
(or aborts the transaction in the case of errors). Additionally,
retries OptmisticConcurrencyControlExceptions and hashes the PartiQL statements
with its parameters on client's behalf in order to commit the transaction.

