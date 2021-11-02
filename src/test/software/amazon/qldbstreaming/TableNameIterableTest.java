/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.qldbstreaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TableNameIterableTest {
    private static final IonSystem SYSTEM = IonSystemBuilder.standard().build();

    @Test
    public void testEmptyResult() {
        testRowResult(Collections.emptyList());
    }

    @Test
    public void testOneRowResult() {
        testRowResult(Collections.singletonList("table1"));
    }

    @Test
    public void testTwoRowResult() {
        testRowResult(Arrays.asList("table1", "table2"));
    }

    @Test
    public void testIncorrectStructureNoStringResult() {
        final List<IonValue> ionTables = new ArrayList<>();
        final IonStruct struct = SYSTEM.newEmptyStruct();
        struct.add("name", SYSTEM.singleValue("table"));
        ionTables.add(struct);

        assertThrows(IllegalStateException.class,
            () -> iterateTables(ionTables));
    }

    private void iterateTables(List<IonValue> ionTables) {
        final Result result = Mockito.mock(Result.class);
        Mockito.when(result.iterator()).thenReturn(ionTables.iterator());

        final TableNameIterable itr = new TableNameIterable(result);
        final Iterator<String> nameItr = itr.iterator();
        assertTrue(nameItr.hasNext());
        nameItr.next();
    }

    private void testRowResult(List<String> tables) {
        final List<IonValue> ionTables = tables.stream().map(SYSTEM::newString).collect(Collectors.toList());

        final Result result = Mockito.mock(Result.class);
        Mockito.when(result.iterator()).thenReturn(ionTables.iterator());

        final TableNameIterable itr = new TableNameIterable(result);
        final Iterator<String> nameItr = itr.iterator();
        for (String table : tables) {
            assertTrue(nameItr.hasNext());
            assertEquals(table, nameItr.next());
        }
        assertFalse(nameItr.hasNext());
    }
}
