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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.amazon.ion.IonInt;
import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import com.amazon.ion.ValueFactory;
import com.amazon.ion.system.IonSystemBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.qldbsession.model.BadRequestException;
import software.amazon.awssdk.services.qldbsession.model.OccConflictException;
import software.amazon.qldb.IOUsage;
import software.amazon.qldb.QldbDriver;
import software.amazon.qldb.Result;
import software.amazon.qldb.TimingInformation;

public class StatementExecutionIntegTest {
    private static LedgerManager ledgerManager;
    private static QldbDriver driver;
    private static ValueFactory valueFactory = IonSystemBuilder.standard().build();

    @BeforeAll
    public static void setup() throws InterruptedException {
        ledgerManager = new LedgerManager(Constants.LEDGER_NAME+System.getProperty("ledgerSuffix"), System.getProperty("region"));

        ledgerManager.runCreateLedger();

        driver = ledgerManager.createQldbDriver(Constants.DEFAULT, Constants.RETRY_LIMIT);

        // Create table
        String createTableQuery = String.format("CREATE TABLE %s", Constants.TABLE_NAME);
        int createTableCount = driver.execute(
            txn -> {
                Result result = txn.execute(createTableQuery);

                int count = 0;
                for (IonValue row : result) {
                    count++;
                }
                return count;
            });
        assertEquals(1, createTableCount);
        Iterable<String> result = driver.getTableNames();
        for (String tableName : result) {
            assertEquals(Constants.TABLE_NAME, tableName);
        }
    }

    @AfterAll
    public static void classCleanup() throws Exception {
        ledgerManager.deleteLedger();
        driver.close();
    }

    @AfterEach
    public void testCleanup() {
        // Delete everything from table after each test
        String deleteQuery = String.format("DELETE FROM %s", Constants.TABLE_NAME);
        driver.execute(txn -> { txn.execute(deleteQuery); });
    }

    @Test
    public void execute_DropExistingTable_TableDropped() {
        // Given
        String createTableQuery = String.format("CREATE TABLE %s", Constants.CREATE_TABLE_NAME);
        int createTableCount = driver.execute(
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
        Iterable<String> result = driver.getTableNames();
        List<String> tables = new ArrayList<>();
        for (String tableName : result) {
            tables.add(tableName);
        }
        assertTrue(tables.contains(Constants.CREATE_TABLE_NAME));

        // When
        String dropTableQuery = String.format("DROP TABLE %s", Constants.CREATE_TABLE_NAME);
        int dropTableCount = driver.execute(
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
        result = driver.getTableNames();
        for (String tableName : result) {
            tables.add(tableName);
        }
        assertFalse(tables.contains(Constants.CREATE_TABLE_NAME));
    }

    @Test
    public void execute_ListTables_ReturnsListOfTables() {
        // When
        Iterable<String> result = driver.getTableNames();

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
        try {
            driver.execute(txn -> { txn.execute(createTableQuery); });
        } catch (Exception e) {
            assertTrue(e instanceof BadRequestException);

            return;
        }

        fail("Test should have thrown BadRequestException.");
    }

    @Test
    public void execute_CreateIndex_IndexIsCreated() {
        // Given
        String createIndexQuery = String.format("CREATE INDEX on %s (%s)", Constants.TABLE_NAME, Constants.INDEX_ATTRIBUTE);

        // When
        int createIndexCount = driver.execute(
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
        String searchValue = driver.execute(
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
        int resultSetSize = driver.execute(
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
        int insertCount = driver.execute(
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
        String searchValue = driver.execute(
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
        int insertCount = driver.execute(
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
        String searchValue = driver.execute(
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
        int insertCount = driver.execute(
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
        String searchValue = driver.execute(
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
        int insertCount = driver.execute(
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
        List<String> searchValues = driver.execute(
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
        int insertCount = driver.execute(
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
        int deletedCount = driver.execute(
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
        int searchCount = driver.execute(
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
        int insertCount = driver.execute(
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
        int deleteCount = driver.execute(
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
        int searchCount = driver.execute(
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
    public void execute_UpdateSameRecordAtSameTime_ThrowsOccException() {
        // Insert document for testing OCC
        IonStruct ionStruct = valueFactory.newEmptyStruct();
        ionStruct.add(Constants.COLUMN_NAME, valueFactory.newInt(0));
        String insertQuery = String.format("INSERT INTO %s ?", Constants.TABLE_NAME);
        int insertCount = driver.execute(
                txn -> {
                    Result result = txn.execute(insertQuery, ionStruct);
                    int count = 0;
                    for (IonValue row : result) {
                        count++;
                    }
                    return count;
                });
        assertEquals(1, insertCount);

        String selectQuery = String.format("SELECT VALUE %s FROM %s", Constants.COLUMN_NAME, Constants.TABLE_NAME);
        String updateQuery = String.format("UPDATE %s SET %s = ?", Constants.TABLE_NAME, Constants.COLUMN_NAME);

        try {
            // For testing purposes only. Forcefully causes an OCC conflict to occur.
            // Do not invoke pooledQldbDriver.execute within the lambda function under normal circumstances.
            driver.execute(
                    txn -> {
                        // Query table
                        Result result = txn.execute(selectQuery);
                        int intValue = 0;
                        for (IonValue ionVal : result) {
                            intValue = ((IonInt) ionVal).intValue();
                        }

                        IonInt ionInt = valueFactory.newInt(intValue + 5);
                        driver.execute(
                                txn2 -> {
                                    // Update document
                                    txn2.execute(updateQuery, ionInt);
                                }
                        );
                    });
        } catch (Exception e) {
            assertTrue(e instanceof OccConflictException);

        }

        // Update document to make sure everything still works after the OCC exception.
        AtomicInteger updatedValue = new AtomicInteger();
        driver.execute(
                txn -> {
                    Result result = txn.execute(selectQuery);
                    int intValue = 0;
                    for (IonValue ionVal : result) {
                        intValue = ((IonInt) ionVal).intValue();
                    }
                    updatedValue.set(intValue + 5);
                    IonInt ionInt = valueFactory.newInt(updatedValue.get());
                    txn.execute(updateQuery, ionInt);
                });
        int intVal = driver.execute(
                txn -> {
                    Result result = txn.execute(selectQuery);
                    int intValue = 0;
                    for (IonValue ionVal : result) {
                        intValue = ((IonInt) ionVal).intValue();
                    }
                    return intValue;
                });
        assertEquals(updatedValue.get(), intVal);
    }

    @Test
    public void execute_ExecuteLambdaThatDoesNotReturnValue_RecordIsUpdated() {
        // Given
        // Create Ion struct to insert
        IonStruct ionStruct = valueFactory.newEmptyStruct();
        ionStruct.add(Constants.COLUMN_NAME, valueFactory.newString(Constants.SINGLE_DOCUMENT_VALUE));

        // When
        String insertQuery = String.format("INSERT INTO %s ?", Constants.TABLE_NAME);
        driver.execute(txn -> { txn.execute(insertQuery, ionStruct); });

        // Then
        String searchQuery = String.format("SELECT VALUE %s FROM %s WHERE %s = '%s'",
            Constants.COLUMN_NAME, Constants.TABLE_NAME, Constants.COLUMN_NAME, Constants.SINGLE_DOCUMENT_VALUE);
        String searchValue = driver.execute(
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
        try {
            driver.execute(txn -> { txn.execute(deleteQuery); });
        } catch (Exception e) {
            assertTrue(e instanceof BadRequestException);

            return;
        }

        fail("Test should have thrown BadRequestException");
    }

    @Test
    public void execute_ExecutionMetrics() {
        driver.execute(
            txn -> {
                String insertQuery = String.format("INSERT INTO %s << {'col': 1}, {'col': 2}, {'col': 3} >>", Constants.TABLE_NAME);
                txn.execute(insertQuery);
            });

        // Given
        String selectQuery = String.format("SELECT * FROM %s as a, %s as b, %s as c, %s as d, %s as e, %s as f",
                Constants.TABLE_NAME, Constants.TABLE_NAME, Constants.TABLE_NAME, Constants.TABLE_NAME, Constants.TABLE_NAME, Constants.TABLE_NAME);

        // When
        driver.execute(
            txn -> {
                Result result = txn.execute(selectQuery);

                for (IonValue row : result) {
                    IOUsage ioUsage = result.getConsumedIOs();
                    TimingInformation timingInfo = result.getTimingInformation();

                    assertNotNull(ioUsage);
                    assertNotNull(timingInfo);

                    assertTrue(ioUsage.getReadIOs() > 0);
                    assertTrue(timingInfo.getProcessingTimeMilliseconds() > 0);
                }
            });

        // When
        Result result = driver.execute(
            txn -> {
                return txn.execute(selectQuery);
            });

        IOUsage ioUsage = result.getConsumedIOs();
        TimingInformation timingInfo = result.getTimingInformation();

        assertNotNull(ioUsage);
        assertNotNull(timingInfo);

        assertEquals(1092, ioUsage.getReadIOs());
        assertTrue(timingInfo.getProcessingTimeMilliseconds() > 0);
    }
}
