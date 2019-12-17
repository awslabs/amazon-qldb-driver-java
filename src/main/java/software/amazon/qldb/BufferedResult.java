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

import com.amazon.ion.IonValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Implementation of a result which buffers all values in memory, rather than stream them from QLDB during retrieval.
 *
 * This implementation should only be used when the result is to be returned after the parent transaction is to be
 * committed.
 */
class BufferedResult implements Result {
    private final List<IonValue> bufferedValues;

    /**
     * Constructor for the result which buffers into the memory the supplied result before closing it.
     *
     * @param result
     *              The result which is to be buffered into memory and closed.
     */
    BufferedResult(Result result) {
        final List<IonValue> tempValues = new ArrayList<>();
        result.iterator().forEachRemaining(tempValues::add);
        bufferedValues = Collections.unmodifiableList(tempValues);
    }

    @Override
    public boolean isEmpty() {
        return bufferedValues.isEmpty();
    }

    @Override
    public Iterator<IonValue> iterator() {
        return bufferedValues.iterator();
    }
}
