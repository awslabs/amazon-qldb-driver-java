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
 * <p>
 * Implementation of a result which buffers all values in memory, rather than stream them from QLDB during retrieval.
 * </p>
 *
 * <p>
 * This implementation should only be used when the result is to be returned after the parent transaction is to be
 * committed.
 * </p>
 */
class BufferedResult implements Result {
    private final List<IonValue> bufferedValues;
    private final IOUsage ioUsage;
    private final TimingInformation timingInfo;

    /**
     * Constructor for the result which buffers into the memory the supplied result before closing it.
     *
     * @param result
     *              The result with the Ion values.
     */
    BufferedResult(Result result) {
        final List<IonValue> tempValues = new ArrayList<>();
        result.iterator().forEachRemaining(tempValues::add);
        this.bufferedValues = Collections.unmodifiableList(tempValues);
        this.ioUsage = result.getConsumedIOs();
        this.timingInfo = result.getTimingInformation();
    }

    @Override
    public boolean isEmpty() {
        return bufferedValues.isEmpty();
    }

    /**
     * Gets the IOUsage statistics for the current statement.
     *
     * @return The current IOUsage statistics.
     */
    @Override
    public IOUsage getConsumedIOs() {
        return ioUsage;
    }

    /**
     * Gets the server side timing information for the current statement.
     *
     * @return The current TimingInformation statistics.
     */
    @Override
    public TimingInformation getTimingInformation() {
        return timingInfo;
    }

    @Override
    public Iterator<IonValue> iterator() {
        return bufferedValues.iterator();
    }
}
