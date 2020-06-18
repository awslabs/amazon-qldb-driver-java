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

import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.awscore.AwsResponseMetadata;
import software.amazon.awssdk.awscore.DefaultAwsResponseMetadata;
import software.amazon.awssdk.awscore.util.AwsHeader;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.qldbsession.model.AbortTransactionResult;
import software.amazon.awssdk.services.qldbsession.model.CommitTransactionResult;
import software.amazon.awssdk.services.qldbsession.model.EndSessionResult;
import software.amazon.awssdk.services.qldbsession.model.ExecuteStatementResult;
import software.amazon.awssdk.services.qldbsession.model.Page;
import software.amazon.awssdk.services.qldbsession.model.SendCommandResponse;
import software.amazon.awssdk.services.qldbsession.model.StartSessionResult;
import software.amazon.awssdk.services.qldbsession.model.StartTransactionResult;
import software.amazon.awssdk.services.qldbsession.model.ValueHolder;

public class MockResponses {
    public static final String SESSION_TOKEN = "sessionToken";
    public static final String REQUEST_ID = "requestId";
    public static final SendCommandResponse ABORT_RESPONSE =
        addRequestId(SendCommandResponse.builder().abortTransaction(AbortTransactionResult.builder().build()));
    public static final StartSessionResult startSessionResult =
        StartSessionResult.builder().sessionToken(SESSION_TOKEN).build();
    public static final SendCommandResponse START_SESSION_RESPONSE =
        addRequestId(SendCommandResponse.builder().startSession(startSessionResult));

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

    public static SendCommandResponse commitTransactionResponse(ByteBuffer commitDigest) {
        return addRequestId(SendCommandResponse.builder().commitTransaction(
            CommitTransactionResult
                .builder()
                .commitDigest(
                    SdkBytes
                        .fromByteBuffer(commitDigest))
                .build()));
    }

    public static SendCommandResponse executeResponse(ExecuteStatementResult result) {
        return addRequestId(SendCommandResponse.builder().executeStatement(result));
    }

    public static SendCommandResponse executeResponse(List<IonValue> results) throws IOException {
        final Page page = Page.builder().nextPageToken(null).values(createByteValues(results)).build();
        final ExecuteStatementResult result = ExecuteStatementResult.builder()
                                                                    .firstPage(page).build();
        return addRequestId(SendCommandResponse.builder().executeStatement(result));
    }

    public static SendCommandResponse startTxnResponse(String id) {
        final StartTransactionResult startTransaction = StartTransactionResult.builder().transactionId(id).build();
        return addRequestId(SendCommandResponse.builder().startTransaction(startTransaction));
    }

    public static SendCommandResponse endSessionResponse() {
        return addRequestId(SendCommandResponse.builder().endSession(EndSessionResult.builder().build()));
    }

    private static SendCommandResponse addRequestId(SendCommandResponse.Builder result) {
        final Map<String, String> responseMap = new HashMap<>();
        responseMap.put(AwsHeader.AWS_REQUEST_ID, REQUEST_ID);

        AwsResponseMetadata awsResponseMetadata = DefaultAwsResponseMetadata.create(responseMap);
        result.responseMetadata(awsResponseMetadata);
        return result.build();
    }
}
