package software.amazon.qldb;

import software.amazon.awssdk.services.qldbsessionv2.model.CommandResult;
import software.amazon.awssdk.services.qldbsessionv2.model.ExecuteStatementResult;
import software.amazon.awssdk.services.qldbsessionv2.model.FetchPageResult;
import software.amazon.awssdk.services.qldbsessionv2.model.ResultStream;
import software.amazon.qldb.exceptions.QldbDriverException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

class ResultStreamSubscriber extends SyncSubscriber<ResultStream> {

    private final LinkedBlockingQueue<CommandResult> commandResults;
    // A map between the next page token of first page and following pages
    private final Map<String, LinkedBlockingQueue<FetchPageResult>> pagesBuffer;
    private CommandResult lastResult;

    protected ResultStreamSubscriber() {
        this.commandResults = new LinkedBlockingQueue<>();
        this.pagesBuffer = new HashMap<>();
    }

    CommandResult waitForResult() throws InterruptedException {
        CommandResult result = commandResults.poll(5000L, TimeUnit.MILLISECONDS);
        if (result == null) {
            System.out.println(Thread.currentThread().getName() + "Timeout waiting for result.");
        }

        assert result != null;
        if (result.commitTransaction() != null) {
            System.out.println("Clean pages buffer");
            pagesBuffer.clear();
        }
        return result;
    }

    LinkedBlockingQueue<FetchPageResult> getPages(String nextPageToken) {
        if (!pagesBuffer.containsKey(nextPageToken)) {
            throw QldbDriverException.create("Incorrect page token");
        }
        return pagesBuffer.get(nextPageToken);
    }

    protected void whenReceived(ResultStream resultStream) {

        CommandResult commandResult = (CommandResult) resultStream;
        // If the result received is FetchPageResult, buffer all the following pages.
        if (commandResult.fetchPage() != null) {
            if (lastResult != null && lastResult.executeStatement() != null) {
                enqueuePage(lastResult.executeStatement().firstPage().nextPageToken(), commandResult.fetchPage());
            }
        } else {
            commandResults.offer(commandResult);
            lastResult = commandResult;
        }
        System.out.println(Thread.currentThread().getName() + " Last result: " + lastResult);
        System.out.println(Thread.currentThread().getName() + " Pages buffer: " + pagesBuffer);
        subscription.request(1);
    }

    private void enqueuePage(String nextPageToken, FetchPageResult page) {
        if (pagesBuffer.get(nextPageToken) != null) {
            pagesBuffer.get(nextPageToken).offer(page);
        } else {
            LinkedBlockingQueue<FetchPageResult> fetchPageResults = new LinkedBlockingQueue<>();
            fetchPageResults.offer(page);
            pagesBuffer.put(nextPageToken, fetchPageResults);
        }
    }
}
