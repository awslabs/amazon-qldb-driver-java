package software.amazon.qldb;

import software.amazon.awssdk.services.qldbsessionv2.model.CommandResult;
import software.amazon.awssdk.services.qldbsessionv2.model.ExecuteStatementResult;
import software.amazon.awssdk.services.qldbsessionv2.model.FetchPageResult;
import software.amazon.awssdk.services.qldbsessionv2.model.ResultStream;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

class ResultStreamSubscriber extends SyncSubscriber<ResultStream> {

    private final LinkedBlockingQueue<CommandResult> commandResults;
    // A map between the next page token of first page and following pages
    private final Map<String, LinkedBlockingQueue<FetchPageResult>> pagesBuffer;

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
        return pagesBuffer.get(nextPageToken);
    }

    protected void whenReceived(ResultStream resultStream) {
        CommandResult latestResult = null;
        if (commandResults.size() > 0) {
            latestResult = (CommandResult) commandResults.toArray()[commandResults.size() - 1];
        }
        CommandResult commandResult = (CommandResult) resultStream;
        // If the result received is FetchPageResult, buffer all the following pages.
        if (commandResult.fetchPage() != null) {
            if (latestResult != null && latestResult.executeStatement() != null) {
                enqueuePage(latestResult.executeStatement(), commandResult.fetchPage());
            }
        } else {
            commandResults.offer(commandResult);
        }
        System.out.println("lastResult: " + latestResult);
        System.out.println("Buffer: " + pagesBuffer);
        System.out.println("CommandsQueue: " + commandResults);
        subscription.request(1);
    }

    private void enqueuePage(ExecuteStatementResult executeResult, FetchPageResult page) {
        String nextPageToken = executeResult.firstPage().nextPageToken();
        if (pagesBuffer.get(nextPageToken) != null) {
            pagesBuffer.get(nextPageToken).offer(page);
        } else {
            LinkedBlockingQueue<FetchPageResult> fetchPageResults = new LinkedBlockingQueue<>();
            fetchPageResults.offer(page);
            pagesBuffer.put(nextPageToken, fetchPageResults);
        }
    }
}
