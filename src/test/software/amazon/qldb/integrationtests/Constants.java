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

package software.amazon.qldb.integrationtests;

public class Constants {
    public static final String TABLE_NAME = "JavaTestTable";
    public static final String CREATE_TABLE_NAME = "JavaCreateTestTable";
    public static final String NON_EXISTENT_TABLE_NAME = "JavaNonExistentTable";
    public static final String COLUMN_NAME = "ColumnName";
    public static final String INDEX_ATTRIBUTE = "IndexName";
    public static final String LEDGER_NAME = "JavaTestLedger";
    public static final String NON_EXISTENT_LEDGER_NAME = "JavaNonExistentLedger";
    public static final String SINGLE_DOCUMENT_VALUE = "SingleDocumentValue";
    public static final String MULTIPLE_DOCUMENT_VALUE_1 = "MultipleDocumentValue1";
    public static final String MULTIPLE_DOCUMENT_VALUE_2 = "MultipleDocumentValue2";

    public static final int RETRY_LIMIT = 5;
    public static final Long LEDGER_POLL_PERIOD_MS = 1000L;
    public static final int DEFAULT = -1;

    private Constants() {
        throw new IllegalStateException("Utility Class");
    }
}
