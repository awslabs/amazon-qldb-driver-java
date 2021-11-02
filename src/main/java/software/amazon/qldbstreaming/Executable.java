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

package software.amazon.qldbstreaming;

import com.amazon.ion.IonValue;
import java.util.List;

/**
 * This interface provides the methods to execute PartiQL statements towards a QLDB Ledger.
 */
public interface Executable {

    /**
     * Execute the statement against QLDB and retrieve the result.
     *
     * @param statement The PartiQL statement to be executed against QLDB.
     * @return The result of executing the statement.
     * @throws software.amazon.awssdk.core.exception.SdkException if there is an error executing against QLDB.
     */
    Result execute(String statement);

    /**
     * Execute the statement using the specified parameters against QLDB and retrieve the result.
     *
     * @param statement  The PartiQL statement to be executed against QLDB.
     * @param parameters The parameters to be used with the PartiQL statement, for each ? placeholder in the statement.
     * @return The result of executing the statement.
     * @throws software.amazon.awssdk.core.exception.SdkException if there is an error executing against QLDB.
     */
    Result execute(String statement, List<IonValue> parameters);

    /**
     * Execute the statement using the specified parameters against QLDB and retrieve the result.
     *
     * @param statement  The PartiQL statement to be executed against QLDB.
     * @param parameters The parameters to be used with the PartiQL statement, for each ? placeholder in the statement.
     * @return The result of executing the statement.
     * @throws software.amazon.awssdk.core.exception.SdkException if there is an error executing against QLDB.
     */
    Result execute(String statement, IonValue... parameters);
}
