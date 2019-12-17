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

import com.amazonaws.services.qldbsession.model.Page;

/**
 * Holder object for facilitating passing of results or exceptions from the asynchronous reader thread to the
 * main retriever instance.
 */
class ResultHolder<T> {
    private final Page result;
    private final T associatedValue;

    /**
     * Constructor for passing a result object.
     *
     * The associatedValue portion of the holder will be set to null when using this constructor.
     *
     * @param result
     *              The result of the fetch that should be encapsulated.
     * @param associatedValue
     *              The value associated with a result.
     */
    public ResultHolder(final Page result, final T associatedValue) {
        this.result = result;
        this.associatedValue = associatedValue;
    }

    /**
     * Constructor for passing a result object.
     *
     * The associatedValue portion of the holder will be set to null when using this constructor.
     *
     * @param result
     *              The result of the fetch that should be encapsulated.
     */
    public ResultHolder(final Page result) {
        this.result = result;
        this.associatedValue = null;
    }

    /**
     * Constructor for passing a value associated with a result.
     *
     * The result portion of the holder will be set to null when using this constructor.
     *
     * @param associatedValue
     *              The value associated with a result.
     */
    public ResultHolder(final T associatedValue) {
        this.result = null;
        this.associatedValue = associatedValue;
    }

    /**
     * Retrieve the associated value from this holder.
     *
     * @return The associated value from this holder.
     */
    public T getAssociatedValue() {
        return associatedValue;
    }

    /**
     * Retrieve the result associated with this holder.
     *
     * @return The result associated with this holder.
     */
    public Page getResult() {
        return result;
    }

    @Override
    public String toString() {
        return "ResultHolder(Result: " + result + ", AssociatedValue: " + associatedValue + ")";
    }
}
