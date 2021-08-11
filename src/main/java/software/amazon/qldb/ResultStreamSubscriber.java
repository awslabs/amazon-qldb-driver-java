package software.amazon.qldb;

import software.amazon.awssdk.services.qldbsessionv2.model.CommandResult;
import software.amazon.awssdk.services.qldbsessionv2.model.ResultStream;

import java.util.concurrent.SynchronousQueue;

class ResultStreamSubscriber extends SyncSubscriber<ResultStream> {

    private final SynchronousQueue<CommandResult> result;

    ResultStreamSubscriber() {
        this.result = new SynchronousQueue<>(true);
    }


    CommandResult waitForResult() throws InterruptedException {
        return result.take();
    }


    @Override
    protected void whenReceived(ResultStream resultStream) throws InterruptedException {
        result.put((CommandResult) resultStream);
    }
}
