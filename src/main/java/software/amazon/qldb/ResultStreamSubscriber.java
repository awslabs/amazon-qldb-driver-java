package software.amazon.qldb;

import software.amazon.awssdk.services.qldbsessionv2.model.CommandResult;
import software.amazon.awssdk.services.qldbsessionv2.model.ResultStream;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

class ResultStreamSubscriber extends SyncSubscriber<ResultStream> {

    private final LinkedBlockingQueue<CommandResult> results;

    ResultStreamSubscriber() {
        this.results = new LinkedBlockingQueue<>();
    }


    CommandResult waitForResult() throws InterruptedException {
        CommandResult result = results.poll(5000L, TimeUnit.MILLISECONDS);
        if (result == null) {
            System.out.println("Timeout waiting for result.");
        }
        return result;
    }


    @Override
    protected void whenReceived(ResultStream resultStream) {
        results.offer((CommandResult) resultStream);
    }
}
