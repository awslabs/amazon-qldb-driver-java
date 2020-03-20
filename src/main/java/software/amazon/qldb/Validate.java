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

class Validate {

    private Validate() {
        throw new InstantiationError();
    }

    static void assertIsAtLeastTwo(int num, String fieldName) {
        if (num < 2) {
            throw new IllegalArgumentException(String.format("%s must be 2 or greater. Given value: %d.", fieldName, num));
        }
    }

    static void assertIsNotNegative(int num, String fieldName) {
        if (num < 0) {
            throw new IllegalArgumentException(String.format("%s must be positive. Given value: %d.", fieldName, num));
        }
    }

    static void assertIsTrue(boolean condition, String fieldName) {
        if (!condition) {
            throw new IllegalArgumentException(String.format("%s must be true.", fieldName));
        }
    }

    static void assertPoolLimit(int configurationLimit, int poolLimit, String fieldName) {
        assertIsNotNegative(poolLimit, fieldName);
        if (poolLimit > configurationLimit) {
            throw new IllegalArgumentException(
                String.format("%s must not exceed the amount set in the client builder's configuration of %d. Given value: %d.",
                        fieldName, configurationLimit, poolLimit));
        }
    }
}
