/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

/**
 * IOUsage class containing metrics for the amount of IO requests.
 */
public class IOUsage {
    private final long readIOs;
    private final long writeIOs;

    IOUsage(long readIOs, long writeIOs) {
        this.readIOs = readIOs;
        this.writeIOs = writeIOs;
    }

    IOUsage(software.amazon.awssdk.services.qldbsession.model.IOUsage ioUsage) {
        this.readIOs = ioUsage.readIOs();
        this.writeIOs = ioUsage.writeIOs();
    }

    public long getReadIOs() {
        return readIOs;
    }

    long getWriteIOs() {
        return writeIOs;
    }
}
