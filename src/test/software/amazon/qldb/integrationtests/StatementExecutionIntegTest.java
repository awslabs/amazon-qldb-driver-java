/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
 * the License. A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package software.amazon.qldb.integrationtests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazon.ion.IonInt;
import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import com.amazon.ion.ValueFactory;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazonaws.services.qldbsession.model.BadRequestException;
import com.amazonaws.services.qldbsession.model.OccConflictException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.qldb.PooledQldbDriver;
import software.amazon.qldb.Result;
import software.amazon.qldb.integrationtests.utils.Constants;
import software.amazon.qldb.integrationtests.utils.IntegrationTestBase;

public class StatementExecutionIntegTest {
    private static IntegrationTestBase integrationTestBase;
    private static PooledQldbDriver pooledQldbDriver;
    private static ValueFactory valueFactory = IonSystemBuilder.standard().build();

    @BeforeAll
    public static void setup() throws InterruptedException {
        integrationTestBase = new IntegrationTestBase(Constants.LEDGER_NAME, System.getProperty("region"));

        integrationTestBase.runCreateLedger();

        pooledQldbDriver = integrationTestBase.createQldbDriver(Constants.DEFAULT, Constants.DEFAULT, Constants.DEFAULT);

        // Create table
        String createTableQuery = String.format("CREATE TABLE %s", Constants.TABLE_NAME);
        int createTableCount = pooledQldbDriver.execute(
            txn -> {
                Result result = txn.execute(createTableQuery);

                int count = 0;
                for (IonValue row : result) {
                    count++;
                }
                return count;
            });
        assertEquals(1, createTableCount);

        Iterable<String> result = pooledQldbDriver.getSession().getTableNames();
        for (String tableName : result) {
            assertEquals(Constants.TABLE_NAME, tableName);
        }
    }

    @AfterAll
    public static void classCleanup() throws InterruptedException {
        integrationTestBase.deleteLedger();
        pooledQldbDriver.close();
    }

    @AfterEach
    public void testCleanup() {
        // Delete everything from table after each test
        String deleteQuery = String.format("DELETE FROM %s", Constants.TABLE_NAME);
        pooledQldbDriver.execute(txn -> { txn.execute(deleteQuery); });
    }

    @Test
    public void execute_DropExistingTable_TableDropped() {
        // Given
        String createTableQuery = String.format("CREATE TABLE %s", Constants.CREATE_TABLE_NAME);
        int createTableCount = pooledQldbDriver.execute(
            txn -> {
                Result createTableResult = txn.execute(createTableQuery);

                int count = 0;
                for (IonValue row : createTableResult) {
                    count++;
                }
                return count;
            });
        assertEquals(1, createTableCount);

        // Ensure table is created
        Iterable<String> result = pooledQldbDriver.getSession().getTableNames();
        List<String> tables = new ArrayList<>();
        for (String tableName : result) {
            tables.add(tableName);
        }
        assertTrue(tables.contains(Constants.CREATE_TABLE_NAME));

        // When
        String dropTableQuery = String.format("DROP TABLE %s", Constants.CREATE_TABLE_NAME);
        int dropTableCount = pooledQldbDriver.execute(
            txn -> {
                Result dropTableResult = txn.execute(dropTableQuery);

                int count = 0;
                for (IonValue row : dropTableResult) {
                    count++;
                }
                return count;
            });
        assertEquals(1, dropTableCount);

        // Then
        tables.clear();
        result = pooledQldbDriver.getSession().getTableNames();
        for (String tableName : result) {
            tables.add(tableName);
        }
        assertFalse(tables.contains(Constants.CREATE_TABLE_NAME));
    }

    @Test
    public void execute_ListTables_ReturnsListOfTables() {
        // When
        Iterable<String> result = pooledQldbDriver.getSession().getTableNames();

        // Then
        int count = 0;
        for (String tableName : result) {
            count++;
            assertEquals(Constants.TABLE_NAME, tableName);
        }

        assertEquals(1, count);
    }

    @Test
    public void execute_CreateTableThatAlreadyExist_ThrowBadRequestException() {
        // Given
        String createTableQuery = String.format("CREATE TABLE %s", Constants.TABLE_NAME);

        // When
        assertThrows(BadRequestException.class, () -> pooledQldbDriver.execute(txn -> { txn.execute(createTableQuery); }));
    }

    @Test
    public void execute_CreateIndex_IndexIsCreated() {
        // Given
        String createIndexQuery = String.format("CREATE INDEX on %s (%s)", Constants.TABLE_NAME, Constants.INDEX_ATTRIBUTE);

        // When
        int createIndexCount = pooledQldbDriver.execute(
            txn -> {
                Result result = txn.execute(createIndexQuery);

                int count = 0;
                for (IonValue row : result) {
                    count++;
                }
                return count;
            });
        assertEquals(1, createIndexCount);

        // Then
        String searchQuery = String.format(
            "SELECT VALUE indexes[0] FROM information_schema.user_tables WHERE status = 'ACTIVE' AND name ='%s'",
            Constants.TABLE_NAME);
        String searchValue = pooledQldbDriver.execute(
            txn -> {
                Result result = txn.execute(searchQuery);

                // Extract the index name by querying the information_schema.
                /* This gives:
                {
                    expr: "[MyColumn]"
                }
                */
                String value = "";
                for (IonValue row : result) {
                    IonValue ionValue = ((IonStruct) row).get("expr");
                    value = ((IonString) ionValue).stringValue();
                }
                return value;
            });
        assertEquals(String.format("[%s]", Constants.INDEX_ATTRIBUTE), searchValue);
    }

    @Test
    public void execute_QueryTableThatHasNoRecords_ReturnsEmptyResult() {
        // Given
        String query = String.format("SELECT * FROM %s", Constants.TABLE_NAME);

        // When
        int resultSetSize = pooledQldbDriver.execute(
            txn -> {
                Result result = txn.execute(query);

                int count = 0;
                for (IonValue row : result) {
                    count++;
                }
                return count;
            });

        // Then
        assertEquals(0, resultSetSize);
    }

    @Test
    public void execute_InsertDocument_DocumentIsInserted() {
        // Given
        // Create Ion struct to insert
        IonStruct ionStruct = valueFactory.newEmptyStruct();
        ionStruct.add(Constants.COLUMN_NAME, valueFactory.newString(Constants.SINGLE_DOCUMENT_VALUE));

        // When
        String insertQuery = String.format("INSERT INTO %s ?", Constants.TABLE_NAME);
        int insertCount = pooledQldbDriver.execute(
            txn -> {
                Result result = txn.execute(insertQuery, ionStruct);

                int count = 0;
                for (IonValue row : result) {
                    count++;
                }
                return count;
            });
        assertEquals(1, insertCount);

        // Then
        String searchQuery = String.format("SELECT VALUE %s FROM %s WHERE %s = '%s'",
            Constants.COLUMN_NAME, Constants.TABLE_NAME, Constants.COLUMN_NAME, Constants.SINGLE_DOCUMENT_VALUE);
        String searchValue = pooledQldbDriver.execute(
            txn -> {
                Result result = txn.execute(searchQuery);

                String value = "";
                for (IonValue row : result) {
                    value = ((IonString) row).stringValue();
                }
                return value;
            });
        assertEquals(Constants.SINGLE_DOCUMENT_VALUE, searchValue);
    }

    @Test
    public void execute_QuerySingleField_ReturnsSingleField() {
        // Given
        // Create Ion struct to insert
        IonStruct ionStruct = valueFactory.newEmptyStruct();
        ionStruct.add(Constants.COLUMN_NAME, valueFactory.newString(Constants.SINGLE_DOCUMENT_VALUE));

        String insertQuery = String.format("INSERT INTO %s ?", Constants.TABLE_NAME);
        int insertCount = pooledQldbDriver.execute(
            txn -> {
                Result result = txn.execute(insertQuery, ionStruct);

                int count = 0;
                for (IonValue row : result) {
                    count++;
                }
                return count;
            });
        assertEquals(1, insertCount);

        // When
        String searchQuery = String.format("SELECT VALUE %s FROM %s WHERE %s = '%s'",
            Constants.COLUMN_NAME, Constants.TABLE_NAME, Constants.COLUMN_NAME, Constants.SINGLE_DOCUMENT_VALUE);
        String searchValue = pooledQldbDriver.execute(
            txn -> {
                Result result = txn.execute(searchQuery);

                String value = "";
                for (IonValue row : result) {
                    value = ((IonString) row).stringValue();
                }
                return value;
            });

        // Then
        assertEquals(Constants.SINGLE_DOCUMENT_VALUE, searchValue);
    }

    @Test
    public void execute_QueryTableEnclosedInQuotes_ReturnsResult() {
        // Given
        // Create Ion struct to insert
        IonStruct ionStruct = valueFactory.newEmptyStruct();
        ionStruct.add(Constants.COLUMN_NAME, valueFactory.newString(Constants.SINGLE_DOCUMENT_VALUE));

        String insertQuery = String.format("INSERT INTO %s ?", Constants.TABLE_NAME);
        int insertCount = pooledQldbDriver.execute(
            txn -> {
                Result result = txn.execute(insertQuery, ionStruct);

                int count = 0;
                for (IonValue row : result) {
                    count++;
                }
                return count;
            });
        assertEquals(1, insertCount);

        // When
        String searchQuery = String.format("SELECT VALUE %s FROM \"%s\" WHERE %s = '%s'",
            Constants.COLUMN_NAME, Constants.TABLE_NAME, Constants.COLUMN_NAME, Constants.SINGLE_DOCUMENT_VALUE);
        String searchValue = pooledQldbDriver.execute(
            txn -> {
                Result result = txn.execute(searchQuery);

                String value = "";
                for (IonValue row : result) {
                    value = ((IonString) row).stringValue();
                }
                return value;
            });

        // Then
        assertEquals(Constants.SINGLE_DOCUMENT_VALUE, searchValue);
    }

    @Test
    public void execute_InsertMultipleDocuments_DocumentsInserted() {
        IonString ionString1 = valueFactory.newString(Constants.MULTIPLE_DOCUMENT_VALUE_1);
        IonString ionString2 = valueFactory.newString(Constants.MULTIPLE_DOCUMENT_VALUE_2);

        // Given
        // Create Ion structs to insert
        IonStruct ionStruct1 = valueFactory.newEmptyStruct();
        ionStruct1.add(Constants.COLUMN_NAME, ionString1);

        IonStruct ionStruct2 = valueFactory.newEmptyStruct();
        ionStruct2.add(Constants.COLUMN_NAME, ionString2);

        List<IonValue> parameters = new ArrayList<>();
        parameters.add(ionStruct1);
        parameters.add(ionStruct2);

        // When
        String insertQuery = String.format("INSERT INTO %s <<?,?>>", Constants.TABLE_NAME);
        int insertCount = pooledQldbDriver.execute(
            txn -> {
                Result result = txn.execute(insertQuery, parameters);

                int count = 0;
                for (IonValue row : result) {
                    count++;
                }
                return count;
            });
        assertEquals(2, insertCount);

        // Then
        String searchQuery = String.format("SELECT VALUE %s FROM %s WHERE %s IN (?,?)",
            Constants.COLUMN_NAME, Constants.TABLE_NAME, Constants.COLUMN_NAME);
        List<String> searchValues = pooledQldbDriver.execute(
            txn -> {
                Result result = txn.execute(searchQuery, ionString1, ionString2);

                List<String> values = new ArrayList<>();
                for (IonValue row : result)
                {
                    values.add(((IonString) row).stringValue());
                }
                return values;
            });
        assertTrue(searchValues.contains(Constants.MULTIPLE_DOCUMENT_VALUE_1));
        assertTrue(searchValues.contains(Constants.MULTIPLE_DOCUMENT_VALUE_2));
    }

    @Test
    public void execute_DeleteSingleDocument_DocumentIsDeleted() {
        // Given
        // Create Ion struct to insert
        IonStruct ionStruct = valueFactory.newEmptyStruct();
        ionStruct.add(Constants.COLUMN_NAME, valueFactory.newString(Constants.SINGLE_DOCUMENT_VALUE));

        String insertQuery = String.format("INSERT INTO %s ?", Constants.TABLE_NAME);
        int insertCount = pooledQldbDriver.execute(
            txn -> {
                Result result = txn.execute(insertQuery, ionStruct);

                int count = 0;
                for (IonValue row : result) {
                    count++;
                }
                return count;
            });
        assertEquals(1, insertCount);

        // When
        String delQuery = String.format(
            "DELETE FROM %s WHERE %s = '%s'", Constants.TABLE_NAME, Constants.COLUMN_NAME, Constants.SINGLE_DOCUMENT_VALUE);
        int deletedCount = pooledQldbDriver.execute(
            txn -> {
                Result result = txn.execute(delQuery);

                int count = 0;
                for (IonValue row : result) {
                    count++;
                }
                return count;
            });
        assertEquals(1, deletedCount);

        // Then
        String searchQuery = String.format("SELECT COUNT(*) FROM %s", Constants.TABLE_NAME);
        int searchCount = pooledQldbDriver.execute(
            txn -> {
                Result result = txn.execute(searchQuery);

                int count = -1;
                for (IonValue row : result) {
                    // This gives:
                    // {
                    //    _1: 1
                    // }
                    IonValue ionValue = ((IonStruct) row).get("_1");
                    count = ((IonInt) ionValue).intValue();
                }
                return count;
            });
        assertEquals(0, searchCount);
    }

    @Test
    public void execute_DeleteAllDocuments_DocumentsAreDeleted() {
        // Given
        // Create Ion structs to insert
        IonStruct ionStruct1 = valueFactory.newEmptyStruct();
        ionStruct1.add(Constants.COLUMN_NAME, valueFactory.newString(Constants.MULTIPLE_DOCUMENT_VALUE_1));

        IonStruct ionStruct2 = valueFactory.newEmptyStruct();
        ionStruct2.add(Constants.COLUMN_NAME, valueFactory.newString(Constants.MULTIPLE_DOCUMENT_VALUE_2));

        List<IonValue> parameters = new ArrayList<>();
        parameters.add(ionStruct1);
        parameters.add(ionStruct2);

        String insertQuery = String.format("INSERT INTO %s <<?,?>>", Constants.TABLE_NAME);
        int insertCount = pooledQldbDriver.execute(
            txn -> {
                Result result = txn.execute(insertQuery, parameters);

                int count = 0;
                for (IonValue row : result) {
                    count++;
                }
                return count;
            });
        assertEquals(2, insertCount);

        // When
        String deleteQuery = String.format("DELETE FROM %s", Constants.TABLE_NAME);
        int deleteCount = pooledQldbDriver.execute(
            txn -> {
                Result result = txn.execute(deleteQuery);

                int count = 0;
                for (IonValue row : result) {
                    count++;
                }
                return count;
            });
        assertEquals(2, deleteCount);

        // Then
        String searchQuery = String.format("SELECT COUNT(*) FROM %s", Constants.TABLE_NAME);
        int searchCount = pooledQldbDriver.execute(
            txn -> {
                Result result = txn.execute(searchQuery);

                int count = -1;
                for (IonValue row : result) {
                    // This gives:
                    // {
                    //    _1: 1
                    // }
                    IonValue ionValue = ((IonStruct) row).get("_1");
                    count = ((IonInt) ionValue).intValue();
                }
                return count;
            });
        assertEquals(0, searchCount);
    }

    @Test
    public void execute_ExecuteLambdaThatDoesNotReturnValue_RecordIsUpdated() {
        // Given
        // Create Ion struct to insert
        IonStruct ionStruct = valueFactory.newEmptyStruct();
        ionStruct.add(Constants.COLUMN_NAME, valueFactory.newString(Constants.SINGLE_DOCUMENT_VALUE));

        // When
        String insertQuery = String.format("INSERT INTO %s ?", Constants.TABLE_NAME);
        pooledQldbDriver.execute(txn -> { txn.execute(insertQuery, ionStruct); });

        // Then
        String searchQuery = String.format("SELECT VALUE %s FROM %s WHERE %s = '%s'",
            Constants.COLUMN_NAME, Constants.TABLE_NAME, Constants.COLUMN_NAME, Constants.SINGLE_DOCUMENT_VALUE);
        String searchValue = pooledQldbDriver.execute(
            txn -> {
                Result result = txn.execute(searchQuery);

                String value = "";
                for (IonValue row : result) {
                    value = ((IonString) row).stringValue();
                }
                return value;
            });
        assertEquals(Constants.SINGLE_DOCUMENT_VALUE, searchValue);
    }

    @Test
    public void execute_DeleteTableThatDoesNotExist_ThrowsBadRequestException() {
        // Given
        String deleteQuery = String.format("DELETE FROM %s", Constants.NON_EXISTENT_TABLE_NAME);

        // When
        assertThrows(BadRequestException.class, () -> pooledQldbDriver.execute(txn -> { txn.execute(deleteQuery); }));
    }
}
