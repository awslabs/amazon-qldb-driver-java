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
package software.amazon.qldb;

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
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

public class TestTableNameIterable {
    private static final IonSystem system = IonSystemBuilder.standard().build();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

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
        final IonStruct struct = system.newEmptyStruct();
        struct.add("name", system.singleValue("table"));
        ionTables.add(struct);

        thrown.expect(IllegalStateException.class);

        iterateTables(ionTables);
    }

    private void iterateTables(List<IonValue> ionTables) {
        final Result result = Mockito.mock(Result.class);
        Mockito.when(result.iterator()).thenReturn(ionTables.iterator());

        final TableNameIterable itr = new TableNameIterable(result);
        final Iterator<String> nameItr = itr.iterator();
        Assert.assertTrue(nameItr.hasNext());
        nameItr.next();
    }

    private void testRowResult(List<String> tables) {
        final List<IonValue> ionTables = tables.stream().map(system::newString).collect(Collectors.toList());

        final Result result = Mockito.mock(Result.class);
        Mockito.when(result.iterator()).thenReturn(ionTables.iterator());

        final TableNameIterable itr = new TableNameIterable(result);
        final Iterator<String> nameItr = itr.iterator();
        for (String table : tables) {
            Assert.assertTrue(nameItr.hasNext());
            Assert.assertEquals(table, nameItr.next());
        }
        Assert.assertFalse(nameItr.hasNext());
    }
}
