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

import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.services.qldbsession.model.CommitTransactionResult;
import com.amazonaws.services.qldbsession.model.ExecuteStatementResult;
import com.amazonaws.services.qldbsession.model.Page;
import com.amazonaws.services.qldbsession.model.SendCommandResult;
import com.amazonaws.services.qldbsession.model.StartSessionResult;
import com.amazonaws.services.qldbsession.model.StartTransactionResult;
import com.amazonaws.services.qldbsession.model.ValueHolder;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class MockResponses {
    private MockResponses() { }

    public static final String SESSION_TOKEN = "sessionToken";
    public static final String REQUEST_ID = "requestId";
    public static final SendCommandResult ABORT_RESPONSE = addRequestId(new SendCommandResult());
    public static final StartSessionResult START_SESSION_RESULT = new StartSessionResult().withSessionToken(SESSION_TOKEN);
    public static final SendCommandResult START_SESSION_RESPONSE =
            addRequestId(new SendCommandResult().withStartSession(START_SESSION_RESULT));

    public static List<ValueHolder> createByteValues(List<IonValue> ionList) throws java.io.IOException {
        final List<ValueHolder> byteParameters = new ArrayList<>(ionList.size());
        final IonBinaryWriterBuilder builder = IonBinaryWriterBuilder.standard();
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        final IonWriter writer = builder.build(stream);
        for (IonValue ionValue : ionList) {
            ionValue.writeTo(writer);
            writer.finish();
            byteParameters.add(new ValueHolder().withIonBinary(ByteBuffer.wrap(stream.toByteArray())));

            // Reset the stream so that it can be re-used.
            stream.reset();
        }

        return byteParameters;
    }

    public static SendCommandResult commitTransactionResponse(ByteBuffer commitDigest) {
        return addRequestId(new SendCommandResult().withCommitTransaction(
                new CommitTransactionResult().withCommitDigest(commitDigest)));
    }

    public static SendCommandResult executeResponse(List<IonValue> results) throws IOException {
        final Page page = new Page().withNextPageToken(null).withValues(createByteValues(results));
        final ExecuteStatementResult result = new ExecuteStatementResult()
                .withFirstPage(page);
        return addRequestId(new SendCommandResult().withExecuteStatement(result));
    }

    public static SendCommandResult startTxnResponse(String id) {
        final StartTransactionResult startTransaction = new StartTransactionResult();
        startTransaction.setTransactionId(id);
        return addRequestId(new SendCommandResult().withStartTransaction(startTransaction));
    }

    public static SendCommandResult endSessionResponse() {
        final SendCommandResult result = new SendCommandResult();
        return addRequestId(result);
    }

    private static SendCommandResult addRequestId(SendCommandResult result) {
        final Map<String, String> responseMap = new HashMap<>();
        responseMap.put(ResponseMetadata.AWS_REQUEST_ID, REQUEST_ID);
        responseMap.put(ResponseMetadata.AWS_EXTENDED_REQUEST_ID, "extRequestId");
        result.setSdkResponseMetadata(new ResponseMetadata(responseMap));
        return result;
    }
}
