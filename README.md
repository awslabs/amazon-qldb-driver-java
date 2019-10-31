# Amazon QLDB Driver for Java
This package provides an interface to [Amazon QLDB](https://aws.amazon.com/qldb/) for Java.

With the Amazon QLDB Driver for Java you can create a session with connectivity to a specific ledger in QLDB. This session enables you to execute PartiQL statements and retrieve the results of those statements. You can also take control over transactions to group multiple executions within a transaction.

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/software.amazon.qldb/amazon-qldb-driver-java/badge.svg)](https://maven-badges.herokuapp.com/maven-central/software.amazon.qldb/amazon-qldb-driver-java)
[![Javadoc](https://javadoc.io/badge2/software.amazon.qldb/amazon-qldb-driver-java/javadoc.svg)](https://javadoc.io/doc/software.amazon.qldb/amazon-qldb-driver-java)

## Introduction

### Prerequisites
Please read the [prerequisites](https://docs.aws.amazon.com/en_pv/qldb/latest/developerguide/runningsamples.html#prerequisites-samples) section of the [Amazon QLDB Sample App](https://github.com/aws-samples/amazon-qldb-dmv-sample-java) to set up the dependencies and AWS credentials. 

### Creating a ledger database
To work with Amazon QLDB, first create the ledger. For more information please read [Step 1: Create a New Ledger](https://docs.aws.amazon.com/en_pv/qldb/latest/developerguide/getting-started.java.step-1.html) of the [Amazon QLDB Developer Guide](https://docs.aws.amazon.com/en_pv/qldb/latest/developerguide/what-is.html).

**Note**: the following sample code will create a ledger database, create the 'Vehicle' table with an index and insert records in that table. Check the full [Amazon QLDB Department of Motor Vehicles Sample App](https://github.com/aws-samples/amazon-qldb-dmv-sample-java) GitHub repository to see the complete example code that will create tables, indexes, insert, query and update data, review the history of the records, verify cryptographically the records stored in the ledger database and export the data. Please also read the [Getting Started with the Driver](https://docs.aws.amazon.com/en_pv/qldb/latest/developerguide/getting-started-driver.html) documentation of the [Amazon QLDB Developer Guide](https://docs.aws.amazon.com/en_pv/qldb/latest/developerguide/what-is.html).


First let's define some constants and an IonObjectMapper that will be used to parse data from [ion](http://amzn.GitHub.io/ion-docs/) to a Java POJO. Amazon Ion is a richly-typed, self-describing, hierarchical data serialization format offering interchangeable binary and text representations.

```java
package software.amazon.qldb.tutorial;

import com.amazon.ion.system.IonSystemBuilder;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.ion.IonObjectMapper;
import com.fasterxml.jackson.dataformat.ion.ionvalue.IonValueMapper;

/**
 * Constant values used throughout this tutorial.
 */
public final class Constants {
    public static final String LEDGER_NAME = "vehicle-registration";
    public static final int RETRY_LIMIT = 4;
    public static final String VEHICLE_TABLE_NAME = "Vehicle";
    public static final String VIN_INDEX_NAME = "VIN";
    public static final IonObjectMapper MAPPER = new IonValueMapper(IonSystemBuilder.standard().build());

    private Constants() { }

    static {
        MAPPER.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }
}

```
* See the complete code of the [Constants.java](https://github.com/aws-samples/amazon-qldb-dmv-sample-java/blob/master/src/main/java/software/amazon/qldb/tutorial/Constants.java) class in the [Amazon QLDB Department of Motor Vehicles Sample App](https://github.com/aws-samples/amazon-qldb-dmv-sample-java) GitHub repository.

Then we can create a ledger database as follows:

- Create a class to describe a ledger database:
```java
package software.amazon.qldb.tutorial;

import com.amazonaws.services.qldb.AmazonQLDB;
import com.amazonaws.services.qldb.model.DescribeLedgerRequest;
import com.amazonaws.services.qldb.model.DescribeLedgerResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Describe a QLDB ledger.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
public final class DescribeLedger {
    public static AmazonQLDB client = CreateLedger.getClient();
    public static final Logger log = LoggerFactory.getLogger(DescribeLedger.class);

    private DescribeLedger() { }

    public static void main(final String... args) {
        try {

            describe(Constants.LEDGER_NAME);

        } catch (Exception e) {
            log.error("Unable to describe a ledger!", e);
        }
    }

    /**
     * Describe a ledger.
     *
     * @param name
     *              Name of the ledger to describe.
     * @return {@link DescribeLedgerResult} from QLDB.
     */
    public static DescribeLedgerResult describe(final String name) {
        log.info("Let's describe ledger with name: {}...", name);
        DescribeLedgerRequest request = new DescribeLedgerRequest().withName(name);
        DescribeLedgerResult result = client.describeLedger(request);
        log.info("Success. Ledger description: {}", result);
        return result;
    }
}

```
Link to the [DescribeLedger](https://github.com/aws-samples/amazon-qldb-dmv-sample-java/blob/master/src/main/java/software/amazon/qldb/tutorial/DescribeLedger.java) class.


- Then create a class to Create a ledger database:
```java
0
```
* Full code to create a ledger database of the sample app is in the class [CreateLedger.java](https://github.com/aws-samples/amazon-qldb-dmv-sample-java/blob/master/src/main/java/software/amazon/qldb/tutorial/CreateLedger.java).

### Testing the connection to the ledger database
Once the ledger database is created, we can connect to the database. For more information please read [Step 2: Test Connectivity to the Ledger ](https://docs.aws.amazon.com/en_pv/qldb/latest/developerguide/getting-started.java.step-2.html):
```java
package software.amazon.qldb.tutorial;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.qldbsession.AmazonQLDBSessionClientBuilder;

import software.amazon.qldb.PooledQldbDriver;
import software.amazon.qldb.QldbDriver;
import software.amazon.qldb.QldbSession;
import software.amazon.qldb.exceptions.QldbClientException;

/**
 * Connect to a session for a given ledger using default settings.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
public final class ConnectToLedger {
    public static final Logger log = LoggerFactory.getLogger(ConnectToLedger.class);
    public static AWSCredentialsProvider credentialsProvider;
    public static String endpoint = null;
    public static String ledgerName = Constants.LEDGER_NAME;
    public static String region = null;

    public static PooledQldbDriver driver = createQldbDriver();

    private ConnectToLedger() { }

    /**
     * Create a pooled driver for creating sessions.
     *
     * @return The pooled driver for creating sessions.
     */
    public static PooledQldbDriver createQldbDriver() {
        AmazonQLDBSessionClientBuilder builder = AmazonQLDBSessionClientBuilder.standard();
        if (null != endpoint && null != region) {
            builder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region));
        }
        if (null != credentialsProvider) {
            builder.setCredentials(credentialsProvider);
        }
        return PooledQldbDriver.builder()
                .withLedger(ledgerName)
                .withRetryLimit(Constants.RETRY_LIMIT)
                .withSessionClientBuilder(builder)
                .build();
    }

    /**
     * Connect to a ledger through a {@link QldbDriver}.
     *
     * @return {@link QldbSession}.
     */
    public static QldbSession createQldbSession() {
        return driver.getSession();
    }

    public static void main(final String... args) {
        try (QldbSession qldbSession = createQldbSession()) {
            log.info("Listing table names ");
            for (String tableName : qldbSession.getTableNames()) {
                log.info(tableName);
            }
        } catch (QldbClientException e) {
            log.error("Unable to create session.", e);
        }
    }
}

```
* See the full class to connect to the ledger in the [/ConnectToLedger.java](https://github.com/aws-samples/amazon-qldb-dmv-sample-java/blob/master/src/main/java/software/amazon/qldb/tutorial/ConnectToLedger.java) class.


### Creating a table
Once the ledger is created, we can create a table with an index using the Amazon QLDB Driver for Java. For example, let's create the table "Vehicle" using a PartiQL statement. For more information please read the [PartiQL documentation](https://PartiQL.org/docs.html)
```java
package software.amazon.qldb.tutorial;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.qldb.QldbSession;
import software.amazon.qldb.Result;
import software.amazon.qldb.TransactionExecutor;
import software.amazon.qldb.tutorial.model.SampleData;

/**
 * Create tables in a QLDB ledger.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
public final class CreateTable {
    public static final Logger log = LoggerFactory.getLogger(CreateTable.class);

    private CreateTable() { }

    /**
     * Registrations, vehicles, owners, and licenses tables being created in a single transaction.
     *
     * @param txn
     *              The {@link TransactionExecutor} for lambda execute.
     * @param tableName
     *              Name of the table to be created.
     * @return the number of tables created.
     */
    public static int createTable(final TransactionExecutor txn, final String tableName) {
        log.info("Creating the '{}' table...", tableName);
        final String createTable = String.format("CREATE TABLE %s", tableName);
        final Result result = txn.execute(createTable);
        log.info("{} table created successfully.", tableName);
        return SampleData.toIonValues(result).size();
    }

    public static void main(final String... args) {
        try (QldbSession qldbSession = ConnectToLedger.createQldbSession()) {
            qldbSession.execute(txn -> {
                createTable(txn, Constants.VEHICLE_TABLE_NAME);                
            }, (retryAttempt) -> log.info("Retrying due to OCC conflict..."));
            log.info("Tables created successfully!");
        } catch (Exception e) {
            log.error("Errors creating tables.", e);
        }
    }
}

```
* Link to the code of the [CreateTable](https://github.com/aws-samples/amazon-qldb-dmv-sample-java/blob/master/src/main/java/software/amazon/qldb/tutorial/CreateTable.java) class.


### Creating the index
With the table created we can create an index for the 'Vehicle' table as follows:

```java
package software.amazon.qldb.tutorial;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.qldb.QldbSession;
import software.amazon.qldb.Result;
import software.amazon.qldb.TransactionExecutor;
import software.amazon.qldb.tutorial.model.SampleData;

/**
 * Create indexes on tables in a particular ledger.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
public final class CreateIndex {
    public static final Logger log = LoggerFactory.getLogger(CreateIndex.class);

    private CreateIndex() { }

    /**
     * In this example, create indexes for registrations and vehicles tables.
     *
     * @param txn
     *              The {@link TransactionExecutor} for lambda execute.
     * @param tableName
     *              Name of the table to be created.
     * @param indexAttribute
     *              The index attribute to use.
     * @return the number of tables created.
     */
    public static int createIndex(final TransactionExecutor txn, final String tableName, final String indexAttribute) {
        log.info("Creating an index on {}...", indexAttribute);
        final String createIndex = String.format("CREATE INDEX ON %s (%s)", tableName, indexAttribute);
        final Result r = txn.execute(createIndex);
        return SampleData.toIonValues(r).size();
    }

    public static void main(final String... args) {
        try (QldbSession qldbSession = ConnectToLedger.createQldbSession()) {
            qldbSession.execute(txn -> {
                createIndex(txn, Constants.VEHICLE_TABLE_NAME, Constants.VIN_INDEX_NAME);                
            }, (retryAttempt) -> log.info("Retrying due to OCC conflict..."));
            log.info("Indexes created successfully!");
        } catch (Exception e) {
            log.error("Unable to create indexes.", e);
        }
    }
}

```
* Link to the code of the [CreateIndex](https://github.com/aws-samples/amazon-qldb-dmv-sample-java/blob/master/src/main/java/software/amazon/qldb/tutorial/CreateIndex.java) class.

### Inserting data
To insert the data into the Vehicle table there are 3 things that needs to be done:
- Define a Java POJO class annotated with Jackson-annotations. 
- Create an instance of the Java POJO.
- Start a session and a transaction to insert the data using PartiQL insert statement.


Below is how the Vehicle Java POJO class is defined for the Vehicle table. 
```java
package software.amazon.qldb.tutorial.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a vehicle, serializable to (and from) Ion.
 */
public final class Vehicle {
    private final String vin;
    private final String type;
    private final int year;
    private final String make;
    private final String model;
    private final String color;

    @JsonCreator
    public Vehicle(@JsonProperty("VIN") final String vin,
                   @JsonProperty("Type") final String type,
                   @JsonProperty("Year") final int year,
                   @JsonProperty("Make") final String make,
                   @JsonProperty("Model") final String model,
                   @JsonProperty("Color") final String color) {
        this.vin = vin;
        this.type = type;
        this.year = year;
        this.make = make;
        this.model = model;
        this.color = color;
    }

    @JsonProperty("Color")
    public String getColor() {
        return color;
    }

    @JsonProperty("Make")
    public String getMake() {
        return make;
    }

    @JsonProperty("Model")
    public String getModel() {
        return model;
    }

    @JsonProperty("Type")
    public String getType() {
        return type;
    }

    @JsonProperty("VIN")
    public String getVin() {
        return vin;
    }

    @JsonProperty("Year")
    public int getYear() {
        return year;
    }
}

```
* Link to the [Vehicle](https://github.com/aws-samples/amazon-qldb-dmv-sample-java/blob/master/src/main/java/software/amazon/qldb/tutorial/model/Vehicle.java) class of the DMV Sample App.

Then create the instances of the Vehicle class. Also define a couple of auxiliar methods that will help  to get the documentId of the inserted records. Think of the documentId as a unique identifier assigned to the document. Here is the code to create the instances:

```java
package software.amazon.qldb.tutorial.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.amazon.ion.IonValue;

import software.amazon.qldb.Result;
import software.amazon.qldb.tutorial.Constants;
import software.amazon.qldb.tutorial.qldb.DmlResultDocument;

/**
 * Sample domain objects for use throughout this tutorial.
 */
public final class SampleData {
    // ...
    public static final List<Vehicle> VEHICLES = Collections
            .unmodifiableList(Arrays.asList(new Vehicle("1N4AL11D75C109151", "Sedan", 2011, "Audi", "A5", "Silver"),
                    new Vehicle("KM8SRDHF6EU074761", "Sedan", 2015, "Tesla", "Model S", "Blue"),
                    new Vehicle("3HGGK5G53FM761765", "Motorcycle", 2011, "Ducati", "Monster 1200", "Yellow"),
                    new Vehicle("1HVBBAANXWH544237", "Semi", 2009, "Ford", "F 150", "Black"),
                    new Vehicle("1C4RJFAG0FC625797", "Sedan", 2019, "Mercedes", "CLK 350", "White")));
    // ...

    /**
     * Convert the result set into a list of IonValues.
     *
     * @param result The result set to convert.
     * @return a list of IonValues.
     */
    public static List<IonValue> toIonValues(Result result) {
        final List<IonValue> valueList = new ArrayList<>();
        result.iterator().forEachRemaining(valueList::add);
        return valueList;
    }

    /**
     * Return a list of modified document IDs as strings from a DML {@link Result}.
     *
     * @param result The result set from a DML operation.
     * @return the list of document IDs modified by the operation.
     */
    public static List<String> getDocumentIdsFromDmlResult(final Result result) {
        final List<String> strings = new ArrayList<>();
        result.iterator().forEachRemaining(row -> strings.add(getDocumentIdFromDmlResultDocument(row)));
        return strings;
    }

    /**
     * Convert the given DML result row's document ID to string.
     *
     * @param dmlResultDocument The {@link IonValue} representing the results of a
     *                          DML operation.
     * @return a string of document ID.
     */
    public static String getDocumentIdFromDmlResultDocument(final IonValue dmlResultDocument) {
        try {
            DmlResultDocument result = Constants.MAPPER.readValue(dmlResultDocument, DmlResultDocument.class);
            return result.getDocumentId();
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }
}

```
- Link to the [SampleApp](https://github.com/aws-samples/amazon-qldb-dmv-sample-java/blob/master/src/main/java/software/amazon/qldb/tutorial/model/SampleData.java) class of the DMV Sample App.

* Also create the auxiliar class DmlResultDocument that is used to get the documentId from an IonValue:
```java
package software.amazon.qldb.tutorial.qldb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Contains information about an individual document inserted or modified as a
 * result of DML.
 */
public class DmlResultDocument {

    private String documentId;

    @JsonCreator
    public DmlResultDocument(@JsonProperty("documentId") final String documentId) {
        this.documentId = documentId;
    }

    public String getDocumentId() {
        return documentId;
    }

    @Override
    public String toString() {
        return "DmlResultDocument{" + "documentId='" + documentId + '\'' + '}';
    }
}

```
- Link to the [DmlResultDocument](https://github.com/aws-samples/amazon-qldb-dmv-sample-java/blob/master/src/main/java/software/amazon/qldb/tutorial/qldb/DmlResultDocument.java) class of the DMV Sample App.

... and insert the data:

```java
package software.amazon.qldb.tutorial;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.ion.IonValue;

import software.amazon.qldb.QldbSession;
import software.amazon.qldb.TransactionExecutor;
import software.amazon.qldb.tutorial.model.SampleData;

/**
 * Insert documents into a table in a QLDB ledger.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
public final class InsertDocument {
    public static final Logger log = LoggerFactory.getLogger(InsertDocument.class);

    private InsertDocument() { }

    /**
     * Insert the given list of documents into the specified table and return the document IDs of the inserted documents.
     *
     * @param txn
     *              The {@link TransactionExecutor} for lambda execute.
     * @param tableName
     *              Name of the table to insert documents into.
     * @param documents
     *              List of documents to insert into the specified table.
     * @return a list of document IDs.
     * @throws IllegalStateException if failed to convert documents into an {@link IonValue}.
     */
    public static List<String> insertDocuments(final TransactionExecutor txn, final String tableName,
                                               final List documents) {
        log.info("Inserting some documents in the {} table...", tableName);
        try {
            final String query = String.format("INSERT INTO %s ?", tableName);
            final IonValue ionDocuments = Constants.MAPPER.writeValueAsIonValue(documents);
            final List<IonValue> parameters = Collections.singletonList(ionDocuments);
            return SampleData.getDocumentIdsFromDmlResult(txn.execute(query, parameters));
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }

    public static void main(final String... args) {
        try (QldbSession qldbSession = ConnectToLedger.createQldbSession()) {
            //...
            qldbSession.execute(txn -> {
                //...
                insertDocuments(txn, Constants.VEHICLE_TABLE_NAME, SampleData.VEHICLES);
                //...
            }, (retryAttempt) -> log.info("Retrying due to OCC conflict..."));
            log.info("Documents inserted successfully!");
        } catch (Exception e) {
            log.error("Error inserting or updating documents.", e);
        }
    }
}

```
* Link to the [InsertDocument](https://github.com/aws-samples/amazon-qldb-dmv-sample-java/blob/master/src/main/java/software/amazon/qldb/tutorial/InsertDocument.java) class of the DMV Sample App.

Finally, execute the classes CreateLedger, DescribeLedger, CreateTable, CreateIndex and InsertDocument in that order.

For more information about how to use the driver for Java, please read the [Getting Started with the Amazon QLDB Driver](https://docs.aws.amazon.com/qldb/latest/developerguide/getting-started-driver.html) of the Amazon QLDB documentation.

# Features
* Provides an easy-to-use programming model for working with transactions with Amazon QLDB.
* Provides a session pool so the sessions can be reused when future transactions are sent to the ledger database.

# Getting Started with Amazon QLDB

1. **Sign up for AWS** &mdash; Before you begin, you need an AWS account. For more information about creating an AWS account and retrieving your AWS credentials, see [AWS Account and Credentials][docs-signup] in the AWS SDK for Java Developer Guide.
1. **Minimum requirements** &mdash; To use the Amazon QLDB Driver for Java, you'll need **Java 1.8+**. For more information about Amazon QLDB Driver for Java requirements, see [Java and Amazon QLDB](https://docs.aws.amazon.com/en_pv/qldb/latest/developerguide/getting-started.java.html) in the Amazon QLDB Developer Guide.
1. **Using the Amazon QLDB Driver for Java** &mdash; The best way to get familiar with the Amazon QLDB Driver for Java is to read [Getting Started with the Amazon QLDB Driver](https://docs.aws.amazon.com/qldb/latest/developerguide/getting-started-driver.html) in the [Amazon QLDB Developer Guide](https://docs.aws.amazon.com/qldb/latest/developerguide/what-is.html).
1. **Using Amazon Ion Java** &mdash; Amazon Ion is a richly-typed, self-describing, hierarchical data serialization format offering interchangeable binary and text representations. For more information read the [ion docs](http://amzn.GitHub.io/ion-docs/) for [Amazon Ion Java](https://github.com/amzn/ion-java)
1. **Using PartiQL** &mdash; Amazon QLDB uses PartiQL to send request to the ledger database. You can get started with the [PartiQL Tutorial](https://PartiQL.org/tutorial.html). Also read the [PartiQL Reference](https://docs.aws.amazon.com/en_pv/qldb/latest/developerguide/ql-reference.html) from the Amazon QLDB Developer Guide. 


# Release Notes

### Release 1.0.1 (September 10, 2019)
- Version 1.0.1 of the Amazon QLDB Driver for Java.


# License

This library is licensed under the Apache 2.0 License.
