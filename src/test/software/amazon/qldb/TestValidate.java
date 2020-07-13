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

package software.amazon.qldb;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class TestValidate {
    @Test
    public void testNegativeIsNotNegative() {
        assertThrows(IllegalArgumentException.class,
            () -> Validate.assertIsNotNegative(-1, "val"));
    }

    @Test
    public void testPositiveIsNotNegative() {
        software.amazon.qldb.Validate.assertIsNotNegative(1, "val");
    }

    @Test
    public void testFalseIsTrue() {
        assertThrows(IllegalArgumentException.class,
            () -> software.amazon.qldb.Validate.assertIsTrue(false, "val"));
    }

    @Test
    public void testTrueIsTrue() {
        software.amazon.qldb.Validate.assertIsTrue(true, "val");
    }

    @Test
    public void testPoolLimitValid() {
        software.amazon.qldb.Validate.assertPoolLimit(20, 19, "poolLimit");
    }

    @Test
    public void testPoolLimitNegative() {
        assertThrows(IllegalArgumentException.class,
            () -> software.amazon.qldb.Validate.assertPoolLimit(20, -1, "poolLimit"));
    }

    @Test
    public void testPoolLimitExceedsConfig() {
        assertThrows(IllegalArgumentException.class,
            () -> software.amazon.qldb.Validate.assertPoolLimit(20, 21, "poolLimit"));
    }
}
