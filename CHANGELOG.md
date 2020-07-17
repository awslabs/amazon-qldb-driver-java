# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.1]
- Fix an error message in the PooledQldbDriver when the driver couldn't acquire 
a seesion under the timeout specified. The error message reported that the 
driver waited X seconds instead of milliseconds.

## [1.1.0](https://github.com/awslabs/amazon-qldb-driver-java/compare/v1.0.2...v1.1.0) - 2020-03-20
### Features
- Add the execute method to the PoolQldbDriver.
- Use varargs on the execute methods to pass ion parameters.

## [1.0.2](https://github.com/awslabs/amazon-qldb-driver-java/compare/v1.0.1...v1.0.2) - 2019-12-17
### Bug Fixes
- Bump version of the AWS SDK to 1.11.649
- Fix an issue that will make the driver throw an `InvalidSessionException` when executing a transaction. In the initial release of the driver, if a session becomes invalid while using the `PooledQldbSession`'s `execute` convenience methods, then the transaction will fail completely for the caller, as the `InvalidSessionException` is re-thrown. To prevent the caller from needing to write additional retry logic, the `execute` methods will now transparently replace an invalid session with a new one and retry the transaction but still be limited to the number of retries configured.

### [1.0.1] - 2019-10-31
### Features
- Provides a layer on top of the AWS SDK for Java called `driver` that simplifies how to execute transactions with QLDB. 
- Includes a Pooled driver that manages the sessions.
- Provides a execute method on the session that starts the session, send the PartiQL statements to QLDB and commits or abort the transactio in the case of errors. Additionally, retries OptmisticConcurrencyControlExceptions and hashes the PartiQL statements with its parameters on customer behalf in order to commit the transaction.

