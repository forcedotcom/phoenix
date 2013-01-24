package phoenix.schema;

/**
 * 
 * Exception thrown when we have a numeric ScanKey value that overflows its arithmetic type 
 * value boundery.
 * 
 * @author zhuang
 * @since 0.1
 */
public class ScanKeyOverflowException extends RuntimeException {

    public ScanKeyOverflowException() { };

    public ScanKeyOverflowException(String message) {
        super(message);
    }

    public ScanKeyOverflowException(Throwable cause) {
        super(cause);
    }

    public ScanKeyOverflowException(String message, Throwable cause) {
        super( message, cause);
    }
}
