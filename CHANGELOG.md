# 2.2.0

### :hammer_and_wrench: Improvements
* Update AWS SDK dependency version to [2.15.79](https://github.com/aws/aws-sdk-java-v2/blob/master/CHANGELOG.md#21579-2021-02-09) which supports [CapacityExceededException](https://docs.aws.amazon.com/qldb/latest/developerguide/driver-errors.html). This will better inform users that they are overloading their ledger.
* Update Ion dependency version to [1.8.0](https://github.com/amzn/ion-java/releases/tag/v1.8.0) which fixes a bug where the binary reader was throwing an error when the user requested more data than available.
* Improved retry logic:
    * Now handles retrying on failure to start a session.
    * Reduce latency by lowering the number of calls to determine session health.
    
### :bug: Fixed
* Fix broken GitHub links in POM.xml.

# [2.1.0](https://github.com/awslabs/amazon-qldb-driver-java/releases/tag/v2.1.0)
Add support for obtaining basic server-side statistics on individual statement executions.

### :tada: Added
* Added `IOUsage` and `TimingInformation` classes to provide server-side execution statistics
   * IOUsage provides `long getReadIOs()`
   * TimingInformation provides `long getProcessingTimeMilliseconds()`
   * Added `IOUsage getConsumedIOs()` and `TimingInformation getTimingInformation()` to the `Result` interface implemented by `BufferedResult` and `StreamResult`
   * `IOUsage getConsumedIOs()` and `TimingInformation getTimingInformation()` methods are stateful, meaning the statistics returned by them reflect the state at the time of method execution

# [2.0.0](https://github.com/awslabs/amazon-qldb-driver-java/releases/tag/v2.0.0)
This is a public and generally available(GA) release of the driver, and this version can be used in production applications.

#### Announcements
The release candidate version [2.0.0-rc.2](https://github.com/awslabs/amazon-qldb-driver-java/releases/tag/v2.0.0-rc.2) 
has been selected as a final release of v2.0.0. No new changes have been introduced between 2.0.0-rc.2 and 2.0.0.

# [2.0.0-rc.2](https://github.com/awslabs/amazon-qldb-driver-java/releases/tag/v2.0.0-rc.2)

#### Bug Fixes:
* Fixed bug which may lead to infinite number of retries when a transaction expires.
* Fixed bug which causes transaction to remain open when an unknown exception is thrown 
inside execute.
* Added a limit to the number of times the driver will try to get (from pool)/create a session.

# [2.0.0-rc.1](https://github.com/awslabs/amazon-qldb-driver-java/releases/tag/v2.0.0-rc.1) 

We are adding new changes to the QLDB Driver for Java. However Java driver 1.x will
still be maintained and supported until further notice. We recommend to move to the version
2.0 as it improves the performance and transaction management.

### :tada:Added 

* Added the `getTableNames` method to the `QldbDriver` class. For more details please
read the [release
notes](https://github.com/awslabs/amazon-qldb-driver-java/releases/tag/v2.0.0-rc.1).
* Added a `RetryPolicy` for transactions. Now, the driver accepts a `RetryPolicy`
instead of the retryLimit that allows you to define the number of retry attempts
and the backoff strategy.

### :hammer_and_wrench: Improvements

* Session pooling functionality moved to `QldbDriver`.  For more details please
read the [release
notes](https://github.com/awslabs/amazon-qldb-driver-java/releases/tag/v2.0.0-rc.1).
* Upgraded dependency from the AWS SDK for Java v1 to the AWS SDK for Java v2. Note,
the current driver version v1.x is still supported and this version doesn't
deprecate it. For more details please read the [release
notes](https://github.com/awslabs/amazon-qldb-driver-java/releases/tag/v2.0.0-rc.1).


### :bug: Fixed 
* Fixed the issue where two different Ion Java packages were present in the classpath.

### :rotating_light:Breaking Changes

* `PooledQldbDriver` has been removed. Please use `QldbDriver` instead. For more
details please read the [release
notes](https://github.com/awslabs/amazon-qldb-driver-java/releases/tag/v2.0.0-rc.1).

* `QldbSession` and `Transaction` classes have been removed.  Please use
`QldbDriver` instead. For more details please read the [release
notes](https://github.com/awslabs/amazon-qldb-driver-java/releases/tag/v2.0.0-rc.1).

* `QldbDriver.execute(String)` method has been removed as we found it made the 
driver confusing about when to use one execute method over the other. Please use `QldbDriver
.execute(ExecutorNoReturn executor)` method  or `QldbDriver.execute(Executor executor)` instead.

* The `RetryIndicator` has been removed in favor of the `RetryPolicy`.  

* `PooledQldbDriverBuilder.poolTimeout` has been removed. We consider that it was confusing and decided to remove it as
 it addded marginal value.

* `PooledQldbDriverBuilder.poolLimit` has been renamed to `QldbDriverBuilder.maxConcurrentTransactions`. We made 
 this change as we think that makes the driver easier to understand and use.

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

