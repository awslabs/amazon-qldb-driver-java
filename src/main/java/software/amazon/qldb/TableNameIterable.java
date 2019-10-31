/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
 * the License. A copy of the License is located at
 *
 * http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package software.amazon.qldb;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazonaws.annotation.NotThreadSafe;

import software.amazon.qldb.exceptions.Errors;

/**
 * Iterable class to create an iterator over a result representing the list of tables within a ledger.
 *
 * Does the conversion from Ion values to strings, such that the iterator returns the string representation of the table
 * names, rather than the IonValue representation.
 */
@NotThreadSafe
class TableNameIterable implements Iterable<String> {
    private static final Logger logger = LoggerFactory.getLogger(TableNameIterable.class);

    private final Iterable<IonValue> iteratorSeed;

    /**
     * Constructs a new iterable object for a result containing the table names.
     *
     * @param result
     *              The result object containing the table names.
     */
    TableNameIterable(Result result) {
        if (!(result instanceof BufferedResult)) {
            // Ensure that the underlying collection cannot be changed during iteration.
            result = new BufferedResult(result);
        }
        this.iteratorSeed = result;
    }

    @Override
    public Iterator<String> iterator() {
        return new TableNameIterator(iteratorSeed.iterator());
    }

    /**
     * Iterator which does the conversion from IonValue to string for the table name result.
     */
    private static class TableNameIterator implements Iterator<String> {
        private final Iterator<IonValue> nameResultIterator;

        /**
         * Constructs a new iterator object over an IonValue iterator, which converts from IonValue to string.
         *
         * @param nameResultIterator
         *              The iterator over the IonValue representation of table names.
         */
        TableNameIterator(Iterator<IonValue> nameResultIterator) {
            this.nameResultIterator = nameResultIterator;
        }

        @Override
        public boolean hasNext() {
            return nameResultIterator.hasNext();
        }

        @Override
        public String next() {
            final IonReader reader = IonReaderBuilder.standard().build(nameResultIterator.next());
            final IonType type = reader.next();
            if (IonType.STRING != type) {
                final String message = String.format(Errors.INCORRECT_TYPE.get(), IonType.STRING, type);
                logger.error(message);
                throw new IllegalStateException(message);
            }

            return reader.stringValue();
        }
    }
}
