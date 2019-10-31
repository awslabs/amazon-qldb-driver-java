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

import org.junit.Test;

import software.amazon.qldb.Validate;

public class TestValidate {
    @Test(expected = IllegalArgumentException.class)
    public void testNegativeIsNotNegative() {
        Validate.assertIsNotNegative(-1, "val");
    }

    @Test
    public void testPositiveIsNotNegative() {
        Validate.assertIsNotNegative(1, "val");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFalseIsTrue() {
        Validate.assertIsTrue(false, "val");
    }

    @Test
    public void testTrueIsTrue() {
        Validate.assertIsTrue(true, "val");
    }

    @Test
    public void testPoolLimitValid() {
        Validate.assertPoolLimit(20, 19, "poolLimit");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPoolLimitNegative() {
        Validate.assertPoolLimit(20, -1, "poolLimit");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPoolLimitExceedsConfig() {
        Validate.assertPoolLimit(20, 21, "poolLimit");
    }
}
