# Amazon QLDB Driver for Java
This package provides an interface to [Amazon QLDB](https://aws.amazon.com/qldb/) for Java.

With the Amazon QLDB Driver for Java you can create a session with connectivity to a specific ledger in QLDB. This 
session enables you to execute PartiQL statements and retrieve the results of those statements. You can also take 
control over transactions to group multiple executions within a transaction.

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/software.amazon.qldb/amazon-qldb-driver-java/badge.svg)](https://maven-badges.herokuapp.com/maven-central/software.amazon.qldb/amazon-qldb-driver-java)
[![Javadoc](https://javadoc.io/badge2/software.amazon.qldb/amazon-qldb-driver-java/javadoc.svg)](https://javadoc.io/doc/software.amazon.qldb/amazon-qldb-driver-java)

Version 1.x is still supported.

## Getting Started

1. **Sign up for AWS** &mdash; Before you begin, you need an AWS account. For more information about creating an AWS 
account and retrieving your AWS credentials, see [AWS Account and Credentials][docs-signup] in the AWS SDK for Java 
Developer Guide.
1. **Minimum requirements** &mdash; To use the Amazon QLDB Driver for Java, you'll need **Java 1.8+**. For more 
information about Amazon QLDB Driver for Java requirements, see [Amazon QLDB Driver for Java](https://docs.aws.amazon.com/en_pv/qldb/latest/developerguide/getting-started.java.html) in the Amazon QLDB Developer Guide.
1. **Install the QLDB driver** &mdash; The driver binaries are in Maven Central. For more information, see [Amazon QLDB documentation for the Java driver](https://docs.aws.amazon.com/qldb/latest/developerguide/getting-started.java.html#getting-started.java.quickstart) section of the Amazon Qldb documentaiton for the java Driver.
1. **Using the Amazon QLDB Driver for Java** &mdash; The best way to get familiar with the Amazon QLDB Driver for Java 
is to read [Getting Started with the Amazon QLDB Driver](https://docs.aws.amazon.com/qldb/latest/developerguide/getting-started-driver.html) in the [Amazon QLDB Developer Guide](https://docs.aws.amazon.com/qldb/latest/developerguide/what-is.html).

## Features
* Provides an easy-to-use programming model for working with transactions with Amazon QLDB.
* Provides a session pool so the sessions can be reused when future transactions are sent to the ledger database.

## See also

1. **Driver Recommendations** &mdash; Check them out in the [Best Practices](https://docs.aws.amazon
.com/qldb/latest/developerguide/driver.best-practices.html) 
in the QLDB documentation.
1. **Exception handling when using QLDB Drivers** &mdash; Refer to the section [Common Errors while using the Amazon 
QLDB Drivers](https://docs.aws.amazon.com/qldb/latest/developerguide/driver-errors.html) 
which describes runtime exceptions that can be thrown by the Amazon QLDB Driver when calling the qldb-session APIs.
1. **Using Amazon Ion Java** &mdash; Amazon Ion is a richly-typed, self-describing, hierarchical data serialization 
format offering interchangeable binary and text representations. For more information read the [Ion docs](http://amzn
.GitHub.io/ion-docs/) for [Amazon Ion Java](https://github.com/amzn/ion-java)
1. **Using PartiQL** &mdash; Amazon QLDB uses PartiQL to send requests to the ledger database. You can get started with 
the [PartiQL Tutorial](https://PartiQL.org/tutorial.html). Also read the [PartiQL Reference](https://docs.aws.amazon.com/en_pv/qldb/latest/developerguide/ql-reference.html) 
from the Amazon QLDB Developer Guide. 

## License

This library is licensed under the Apache 2.0 License.
