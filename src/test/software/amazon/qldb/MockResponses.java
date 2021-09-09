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
import software.amazon.awssdk.services.qldbsessionv2.model.AbortTransactionResult;
import software.amazon.awssdk.services.qldbsessionv2.model.CommandResult;
import software.amazon.awssdk.services.qldbsessionv2.model.CommitTransactionResult;
import software.amazon.awssdk.services.qldbsessionv2.model.EndSessionResult;
import software.amazon.awssdk.services.qldbsessionv2.model.ExecuteStatementResult;
import software.amazon.awssdk.services.qldbsessionv2.model.FetchPageResult;
import software.amazon.awssdk.services.qldbsessionv2.model.Page;
import software.amazon.awssdk.services.qldbsessionv2.model.SendCommandResponse;
import software.amazon.awssdk.services.qldbsessionv2.model.StartTransactionResult;
import software.amazon.awssdk.services.qldbsessionv2.model.ValueHolder;

public class MockResponses {
    public static final String SESSION_TOKEN = "sessionToken";
    public static final String REQUEST_ID = "requestId";

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

    public static SendCommandResponse sendCommandResponse() {
        return SendCommandResponse.builder().build();
    }

    public static CommandResult commitTransactionResponse(ByteBuffer commitDigest) {
        return CommandResult.builder().commitTransaction(
                CommitTransactionResult
                .builder()
                .commitDigest(SdkBytes.fromByteBuffer(commitDigest))
                .build()
        ).build();
    }

    public static CommandResult executeResponse(ExecuteStatementResult result) {
        return CommandResult.builder().executeStatement(result).build();
    }

    public static CommandResult executeResponse(List<IonValue> results) throws IOException {
        final Page page = Page.builder().nextPageToken(null).values(createByteValues(results)).build();
        final ExecuteStatementResult result = ExecuteStatementResult.builder()
                                                                    .firstPage(page).build();
        return CommandResult.builder().executeStatement(result).build();
    }

    public static CommandResult executeResponseWithNextPageToken(String token, List<IonValue> results) throws IOException {
        final Page page = Page.builder().nextPageToken(token).values(createByteValues(results)).build();
        final ExecuteStatementResult result = ExecuteStatementResult.builder()
                .firstPage(page).build();
        return CommandResult.builder().executeStatement(result).build();
    }

    public static CommandResult fetchPageResponseWithOutNextPageToken(List<IonValue> results) throws IOException {
        final Page page = Page.builder().nextPageToken(null).values(createByteValues(results)).build();
        final FetchPageResult result = FetchPageResult.builder().page(page).build();

        return CommandResult.builder().fetchPage(result).build();
    }

    public static CommandResult fetchPageResponse(List<IonValue> results) throws IOException {
        final Page page = Page.builder().nextPageToken("fetchPageToken").values(createByteValues(results)).build();
        final FetchPageResult result = FetchPageResult.builder().page(page).build();

        return CommandResult.builder().fetchPage(result).build();
    }

    public static CommandResult startTxnResponse(String id) {
        final StartTransactionResult startTransaction = StartTransactionResult.builder().transactionId(id).build();
        return CommandResult.builder().startTransaction(startTransaction).build();
    }

    public static CommandResult abortTxnResponse() {
       return CommandResult.builder().abortTransaction(AbortTransactionResult.builder().build()).build();
    }

    public static CommandResult endSessionResponse() {
        return CommandResult.builder().endSession(EndSessionResult.builder().build()).build();
    }

}
