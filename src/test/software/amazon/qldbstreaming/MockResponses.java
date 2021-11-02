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

package software.amazon.qldbstreaming;

import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.qldbsessionv2.model.AbortTransactionResult;
import software.amazon.awssdk.services.qldbsessionv2.model.CommitTransactionResult;
import software.amazon.awssdk.services.qldbsessionv2.model.EndSessionResult;
import software.amazon.awssdk.services.qldbsessionv2.model.ExecuteStatementResult;
import software.amazon.awssdk.services.qldbsessionv2.model.Page;
import software.amazon.awssdk.services.qldbsessionv2.model.SendCommandResponse;
import software.amazon.awssdk.services.qldbsessionv2.model.StartTransactionResult;
import software.amazon.awssdk.services.qldbsessionv2.model.TransactionError;
import software.amazon.awssdk.services.qldbsessionv2.model.ValueHolder;

public class MockResponses {
    public static final String SESSION_ID = "sessionId";
    public static final SendCommandResponse SEND_COMMAND_RESPONSE = SendCommandResponse.builder().sessionId(SESSION_ID).build();
    public static final AbortTransactionResult ABORT_TRANSACTION_RESULT = AbortTransactionResult.builder().build();
    public static final EndSessionResult END_SESSION_RESULT = EndSessionResult.builder().build();

    public static List<ValueHolder> createByteValues(List<IonValue> ionList) throws java.io.IOException {
        final List<ValueHolder> byteParameters = new ArrayList<>(ionList.size());
        final IonBinaryWriterBuilder builder = IonBinaryWriterBuilder.standard();
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        final IonWriter writer = builder.build(stream);
        for (IonValue ionValue : ionList) {
            ionValue.writeTo(writer);
            writer.finish();
            byteParameters.add(ValueHolder.builder().ionBinary(SdkBytes.fromByteArray(stream.toByteArray())).build());

            // Reset the stream so that it can be re-used.
            stream.reset();
        }

        return byteParameters;
    }

    public static CommitTransactionResult commitTransactionResponse(String id) {
        return CommitTransactionResult.builder().transactionId(id).build();
    }

    public static ExecuteStatementResult executeResponse(List<IonValue> results) throws IOException {
        final Page page = Page.builder().nextPageToken(null).values(createByteValues(results)).build();
        return ExecuteStatementResult.builder().firstPage(page).build();
    }

    public static StartTransactionResult startTxnResponse(String id) {
        return StartTransactionResult.builder().transactionId(id).build();
    }

    public static TransactionError transactionErrorResponse(String message, String code) {
        return TransactionError.builder().message(message).code(code).build();
    }
}