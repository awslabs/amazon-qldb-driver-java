# Amazon QLDB Driver for Java

This is the Java driver for Amazon Quantum Ledger Database (QLDB), which allows Java developers to write software that makes use of Amazon QLDB.

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/software.amazon.qldb/amazon-qldb-driver-java/badge.svg)](https://maven-badges.herokuapp.com/maven-central/software.amazon.qldb/amazon-qldb-driver-java)
[![Javadoc](https://javadoc.io/badge2/software.amazon.qldb/amazon-qldb-driver-java/javadoc.svg)](https://javadoc.io/doc/software.amazon.qldb/amazon-qldb-driver-java)
[![Java CI with Maven](https://github.com/awslabs/amazon-qldb-driver-java/actions/workflows/maven.yml/badge.svg)](https://github.com/awslabs/amazon-qldb-driver-java/actions/workflows/maven.yml)

Version 1.x is still supported.

## Getting Started

1. **Sign up for AWS** &mdash; Before you begin, you need an AWS account. For more information about creating an AWS 
account and retrieving your AWS credentials, see [Accessing Amazon QLDB](https://docs.aws.amazon.com/qldb/latest/developerguide/accessing.html) in the Amazon QLDB Developer Guide.
1. **Minimum requirements** &mdash; To use the Amazon QLDB Driver for Java, you'll need **Java 1.8+**. For more 
information about Amazon QLDB Driver for Java requirements, see [Amazon QLDB Driver for Java](https://docs.aws.amazon.com/en_pv/qldb/latest/developerguide/getting-started.java.html) in the Amazon QLDB Developer Guide.
1. **Install the QLDB driver** &mdash; The driver binaries are in Maven Central. For more information, see [Amazon QLDB documentation for the Java driver](https://docs.aws.amazon.com/qldb/latest/developerguide/getting-started.java.html#getting-started.java.quickstart) section of the Amazon QLDB documentation for the Java Driver.
1. **Using the Amazon QLDB Driver for Java** &mdash; The best way to get familiar with the Amazon QLDB Driver for Java 
is to read [Getting Started with the Amazon QLDB Driver](https://docs.aws.amazon.com/qldb/latest/developerguide/getting-started-driver.html) in the [Amazon QLDB Developer Guide](https://docs.aws.amazon.com/qldb/latest/developerguide/what-is.html).

## Getting Help

Please use these community resources for getting help.
* Ask a question on StackOverflow and tag it with the [amazon-qldb](https://stackoverflow.com/questions/tagged/amazon-qldb) tag.
* Open a support ticket with [AWS Support](http://docs.aws.amazon.com/awssupport/latest/user/getting-started.html).
* Make a new thread at [AWS QLDB Forum](https://forums.aws.amazon.com/forum.jspa?forumID=353&start=0).
* If you think you may have found a bug, please open an [issue](https://github.com/awslabs/amazon-qldb-driver-java/issues/new).

## Opening Issues

If you encounter a bug with the Amazon QLDB Java Driver, we would like to hear about it. Please search the [existing issues](https://github.com/awslabs/amazon-qldb-driver-java/issues) and see if others are also experiencing the issue before opening a new issue. When opening a new issue, we will need the version of Amazon QLDB Java Driver, Java language version, and OS you’re using. Please also include reproduction case for the issue when appropriate.

The GitHub issues are intended for bug reports and feature requests. For help and questions with using Amazon QLDB Java Driver, please make use of the resources listed in the [Getting Help](https://github.com/awslabs/amazon-qldb-driver-java#getting-help) section. Keeping the list of open issues lean will help us respond in a timely manner.

## Features
* Provides an easy-to-use programming model for working with transactions with Amazon QLDB.
* Provides a session pool so the sessions can be reused when future transactions are sent to the ledger database.

## See also

1. **Driver Recommendations** &mdash; Check them out in the [Best Practices](https://docs.aws.amazon.com/qldb/latest/developerguide/driver.best-practices.html) 
in the QLDB documentation.
1. **Exception handling when using QLDB Drivers** &mdash; Refer to the section [Common Errors while using the Amazon 
QLDB Drivers](https://docs.aws.amazon.com/qldb/latest/developerguide/driver-errors.html) 
which describes runtime exceptions that can be thrown by the Amazon QLDB Driver when calling the qldb-session APIs.
1. **Using Amazon Ion Java** &mdash; Amazon Ion is a richly-typed, self-describing, hierarchical data serialization 
format offering interchangeable binary and text representations. For more information read the [Ion docs](http://amzn.GitHub.io/ion-docs/) for [Amazon Ion Java](https://github.com/amzn/ion-java).
1. **Using PartiQL** &mdash; Amazon QLDB uses PartiQL to send requests to the ledger database. You can get started with 
the [PartiQL Tutorial](https://PartiQL.org/tutorial.html). Also read the [PartiQL Reference](https://docs.aws.amazon.com/en_pv/qldb/latest/developerguide/ql-reference.html) 
from the Amazon QLDB Developer Guide. 

## License

This library is licensed under the Apache 2.0 License.
