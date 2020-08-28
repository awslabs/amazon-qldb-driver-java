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
import static org.junit.jupiter.api.Assertions.fail;

import com.amazon.ion.IonBlob;
import com.amazon.ion.IonBool;
import com.amazon.ion.IonClob;
import com.amazon.ion.IonDecimal;
import com.amazon.ion.IonFloat;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonList;
import com.amazon.ion.IonNull;
import com.amazon.ion.IonSexp;
import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.Timestamp;
import com.amazon.ion.ValueFactory;
import com.amazon.ion.system.IonSystemBuilder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.qldb.QldbDriver;
import software.amazon.qldb.Result;

public class IonTypesIntegTest {
    private static LedgerManager ledgerManager;
    private static QldbDriver driver;
    private static ValueFactory valueFactory = IonSystemBuilder.standard().build();

    @BeforeAll
    private static void setup() throws InterruptedException {
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
    private static void cleanup() throws Exception {
        ledgerManager.deleteLedger();
        driver.close();
    }

    @AfterEach
    public void testCleanup() {
        // Delete everything from table after each test
        String deleteQuery = String.format("DELETE FROM %s", Constants.TABLE_NAME);
        driver.execute(txn -> { txn.execute(deleteQuery); });
    }

    @ParameterizedTest
    @MethodSource("createIonValues")
    public void execute_InsertAndReadIonTypes_IonTypesAreInsertedAndRead(final IonValue ionValue) {
        // Given
        // Create Ion struct to be inserted
        IonStruct ionStruct = valueFactory.newEmptyStruct();
        ionStruct.add(Constants.COLUMN_NAME, ionValue);

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
        IonValue searchResult;
        if (ionValue.isNullValue()) {
            String searchQuery = String.format("SELECT VALUE %s FROM %s WHERE %s IS NULL",
                Constants.COLUMN_NAME, Constants.TABLE_NAME, Constants.COLUMN_NAME);

            searchResult = driver.execute(
                txn -> {
                    Result result = txn.execute(searchQuery);

                    IonValue value = null;
                    for (IonValue row : result) {
                        value = row;
                    }
                    return value;
                });
        } else {
            String searchQuery = String.format("SELECT VALUE %s FROM %s WHERE %s = ?",
                Constants.COLUMN_NAME, Constants.TABLE_NAME, Constants.COLUMN_NAME);

            searchResult = driver.execute(
                txn -> {
                    Result result = txn.execute(searchQuery, ionValue);

                    IonValue value = null;
                    for (IonValue row : result) {
                        value = row;
                    }
                    return value;
                });
        }

        // Then
        IonType searchType = searchResult.getType();
        IonType ionValType = ionValue.getType();
        if (searchType != ionValType) {
            fail(String.format("The queried value type, %s, does not match %s.", searchType.toString(), ionValType.toString()));
        }
    }

    @ParameterizedTest
    @MethodSource("createIonValues")
    public void execute_UpdateIonTypes_IonTypesAreUpdated(final IonValue ionValue) {
        // Given
        // Create Ion struct to be inserted
        IonStruct ionStruct = valueFactory.newEmptyStruct();
        ionStruct.add(Constants.COLUMN_NAME, ionValue);

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
        String updateQuery = String.format("UPDATE %s SET %s = ?", Constants.TABLE_NAME, Constants.COLUMN_NAME);
        int updateCount = driver.execute(
            txn -> {
                Result result = txn.execute(updateQuery, ionValue);

                int count = 0;
                for (IonValue row : result) {
                    count++;
                }
                return count;
            });
        assertEquals(1, updateCount);

        // Then
        IonValue searchResult;
        if (ionValue.isNullValue()) {
            String searchQuery = String.format("SELECT VALUE %s FROM %s WHERE %s IS NULL",
                Constants.COLUMN_NAME, Constants.TABLE_NAME, Constants.COLUMN_NAME);

            searchResult = driver.execute(
                txn -> {
                    Result result = txn.execute(searchQuery);

                    IonValue value = null;
                    for (IonValue row : result) {
                        value = row;
                    }
                    return value;
                });
        } else {
            String searchQuery = String.format("SELECT VALUE %s FROM %s WHERE %s = ?",
                Constants.COLUMN_NAME, Constants.TABLE_NAME, Constants.COLUMN_NAME);

            searchResult = driver.execute(
                txn -> {
                    Result result = txn.execute(searchQuery, ionValue);

                    IonValue value = null;
                    for (IonValue row : result) {
                        value = row;
                    }
                    return value;
                });
        }

        IonType searchType = searchResult.getType();
        IonType ionValType = ionValue.getType();
        if (searchType != ionValType) {
            fail(String.format("The queried value type, %s, does not match %s.", searchType.toString(), ionValType.toString()));
        }
    }

    private static byte[] getAsciiBytes(final String str) {
        return str.getBytes(StandardCharsets.US_ASCII);
    }

    private static List<IonValue> createIonValues() {
        List<IonValue> ionValues = new ArrayList<>();

        IonBlob ionBlob = valueFactory.newBlob(getAsciiBytes("value"));
        ionValues.add(ionBlob);

        IonBool ionBool = valueFactory.newBool(true);
        ionValues.add(ionBool);

        IonClob ionClob = valueFactory.newClob(getAsciiBytes("{{ 'Clob value.'}}"));
        ionValues.add(ionClob);

        IonDecimal ionDecimal = valueFactory.newDecimal(0.1);
        ionValues.add(ionDecimal);

        IonFloat ionFloat = valueFactory.newFloat(1.1);
        ionValues.add(ionFloat);

        IonInt ionInt = valueFactory.newInt(2);
        ionValues.add(ionInt);

        IonList ionList = valueFactory.newEmptyList();
        ionList.add(valueFactory.newInt(3));
        ionValues.add(ionList);

        IonNull ionNull = valueFactory.newNull();
        ionValues.add(ionNull);

        IonSexp ionSexp = valueFactory.newEmptySexp();
        ionSexp.add(valueFactory.newString("value"));
        ionValues.add(ionSexp);

        IonString ionString = valueFactory.newString("value");
        ionValues.add(ionString);

        IonStruct ionStruct = valueFactory.newEmptyStruct();
        ionStruct.add("value", valueFactory.newBool(true));
        ionValues.add(ionStruct);

        IonSymbol ionSymbol = valueFactory.newSymbol("symbol");
        ionValues.add(ionSymbol);

        IonTimestamp ionTimestamp = valueFactory.newTimestamp(Timestamp.now());
        ionValues.add(ionTimestamp);

        IonBlob ionNullBlob = valueFactory.newNullBlob();
        ionValues.add(ionNullBlob);

        IonBool ionNullBool = valueFactory.newNullBool();
        ionValues.add(ionNullBool);

        IonClob ionNullClob = valueFactory.newNullClob();
        ionValues.add(ionNullClob);

        IonDecimal ionNullDecimal = valueFactory.newNullDecimal();
        ionValues.add(ionNullDecimal);

        IonFloat ionNullFloat = valueFactory.newNullFloat();
        ionValues.add(ionNullFloat);

        IonInt ionNullInt = valueFactory.newNullInt();
        ionValues.add(ionNullInt);

        IonList ionNullList = valueFactory.newNullList();
        ionValues.add(ionNullList);

        IonSexp ionNullSexp = valueFactory.newNullSexp();
        ionValues.add(ionNullSexp);

        IonString ionNullString = valueFactory.newNullString();
        ionValues.add(ionNullString);

        IonStruct ionNullStruct = valueFactory.newNullStruct();
        ionValues.add(ionNullStruct);

        IonSymbol ionNullSymbol = valueFactory.newNullSymbol();
        ionValues.add(ionNullSymbol);

        IonTimestamp ionNullTimestamp = valueFactory.newNullTimestamp();
        ionValues.add(ionNullTimestamp);

        IonBlob ionBlobWithAnnotation = valueFactory.newBlob(getAsciiBytes("value"));
        ionBlobWithAnnotation.addTypeAnnotation("annotation");
        ionValues.add(ionBlobWithAnnotation);

        IonBool ionBoolWithAnnotation = valueFactory.newBool(true);
        ionBoolWithAnnotation.addTypeAnnotation("annotation");
        ionValues.add(ionBoolWithAnnotation);

        IonClob ionClobWithAnnotation = valueFactory.newClob(getAsciiBytes("{{ 'Clob value.'}}"));
        ionClobWithAnnotation.addTypeAnnotation("annotation");
        ionValues.add(ionClobWithAnnotation);

        IonDecimal ionDecimalWithAnnotation = valueFactory.newDecimal(0.1);
        ionDecimalWithAnnotation.addTypeAnnotation("annotation");
        ionValues.add(ionDecimalWithAnnotation);

        IonFloat ionFloatWithAnnotation = valueFactory.newFloat(1.1);
        ionFloatWithAnnotation.addTypeAnnotation("annotation");
        ionValues.add(ionFloatWithAnnotation);

        IonInt ionIntWithAnnotation = valueFactory.newInt(2);
        ionIntWithAnnotation.addTypeAnnotation("annotation");
        ionValues.add(ionIntWithAnnotation);

        IonList ionListWithAnnotation = valueFactory.newEmptyList();
        ionListWithAnnotation.add(valueFactory.newInt(3));
        ionListWithAnnotation.addTypeAnnotation("annotation");
        ionValues.add(ionListWithAnnotation);

        IonNull ionNullWithAnnotation = valueFactory.newNull();
        ionNullWithAnnotation.addTypeAnnotation("annotation");
        ionValues.add(ionNullWithAnnotation);

        IonSexp ionSexpWithAnnotation = valueFactory.newEmptySexp();
        ionSexpWithAnnotation.add(valueFactory.newString("value"));
        ionSexpWithAnnotation.addTypeAnnotation("annotation");
        ionValues.add(ionSexpWithAnnotation);

        IonString ionStringWithAnnotation = valueFactory.newString("value");
        ionStringWithAnnotation.addTypeAnnotation("annotation");
        ionValues.add(ionStringWithAnnotation);

        IonStruct ionStructWithAnnotation = valueFactory.newEmptyStruct();
        ionStructWithAnnotation.add("value", valueFactory.newBool(true));
        ionStructWithAnnotation.addTypeAnnotation("annotation");
        ionValues.add(ionStructWithAnnotation);

        IonSymbol ionSymbolWithAnnotation = valueFactory.newSymbol("symbol");
        ionSymbolWithAnnotation.addTypeAnnotation("annotation");
        ionValues.add(ionSymbolWithAnnotation);

        IonTimestamp ionTimestampWithAnnotation = valueFactory.newTimestamp(Timestamp.now());
        ionTimestampWithAnnotation.addTypeAnnotation("annotation");
        ionValues.add(ionTimestampWithAnnotation);

        IonBlob ionNullBlobWithAnnotation = valueFactory.newNullBlob();
        ionNullBlobWithAnnotation.addTypeAnnotation("annotation");
        ionValues.add(ionNullBlobWithAnnotation);

        IonBool ionNullBoolWithAnnotation = valueFactory.newNullBool();
        ionNullBoolWithAnnotation.addTypeAnnotation("annotation");
        ionValues.add(ionNullBoolWithAnnotation);

        IonClob ionNullClobWithAnnotation = valueFactory.newNullClob();
        ionNullClobWithAnnotation.addTypeAnnotation("annotation");
        ionValues.add(ionNullClobWithAnnotation);

        IonDecimal ionNullDecimalWithAnnotation = valueFactory.newNullDecimal();
        ionNullDecimalWithAnnotation.addTypeAnnotation("annotation");
        ionValues.add(ionNullDecimalWithAnnotation);

        IonFloat ionNullFloatWithAnnotation = valueFactory.newNullFloat();
        ionNullFloatWithAnnotation.addTypeAnnotation("annotation");
        ionValues.add(ionNullFloatWithAnnotation);

        IonInt ionNullIntWithAnnotation = valueFactory.newNullInt();
        ionNullIntWithAnnotation.addTypeAnnotation("annotation");
        ionValues.add(ionNullIntWithAnnotation);

        IonList ionNullListWithAnnotation = valueFactory.newNullList();
        ionNullListWithAnnotation.addTypeAnnotation("annotation");
        ionValues.add(ionNullListWithAnnotation);

        IonSexp ionNullSexpWithAnnotation = valueFactory.newNullSexp();
        ionNullSexpWithAnnotation.addTypeAnnotation("annotation");
        ionValues.add(ionNullSexpWithAnnotation);

        IonString ionNullStringWithAnnotation = valueFactory.newNullString();
        ionNullStringWithAnnotation.addTypeAnnotation("annotation");
        ionValues.add(ionNullStringWithAnnotation);

        IonStruct ionNullStructWithAnnotation = valueFactory.newNullStruct();
        ionNullStructWithAnnotation.addTypeAnnotation("annotation");
        ionValues.add(ionNullStructWithAnnotation);

        IonSymbol ionNullSymbolWithAnnotation = valueFactory.newNullSymbol();
        ionNullSymbolWithAnnotation.addTypeAnnotation("annotation");
        ionValues.add(ionNullSymbolWithAnnotation);

        IonTimestamp ionNullTimestampWithAnnotation = valueFactory.newNullTimestamp();
        ionNullTimestampWithAnnotation.addTypeAnnotation("annotation");
        ionValues.add(ionNullTimestampWithAnnotation);

        return ionValues;
    }
}
