package software.amazon.qldb.exceptions;

import software.amazon.awssdk.services.qldbsessionv2.model.QldbSessionV2Exception;
import software.amazon.awssdk.services.qldbsessionv2.model.StatementError;

/**
 * Exception used to represent all statement related failures that is returned from QLDB service side.
 *
 */
public class StatementException extends QldbSessionV2Exception {
    /**
     * Protected constructor for creating an exception with a specific message wrapping another exception.
     *
     * To create this exception, use one of the builder methods.
     *
     * @param message
     *              The message for this exception.
     * @param statusCode
     *              The cause of this exception.
     */
    protected StatementException(String message, Integer statusCode) {
        super(QldbSessionV2Exception.builder().message(message).statusCode(statusCode));
    }

    protected StatementException(String message) {
        super(QldbSessionV2Exception.builder().message(message));
    }

    /**
     * Factory method for creating an exception with a specific message and status code of StatementError.
     *
     * @param error
     *
     * @return A QldbDriverException with the error message and the status code of the statement execution failure.
     */
    public static StatementException create(StatementError error) {
        if (error.code() == null) {
            return new StatementException(error.message());
        } else return new StatementException(error.message(), Integer.parseInt(error.code()));
    }
}
