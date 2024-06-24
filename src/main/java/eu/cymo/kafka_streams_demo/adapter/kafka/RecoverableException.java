package eu.cymo.kafka_streams_demo.adapter.kafka;

public class RecoverableException extends RuntimeException {
    private static final long serialVersionUID = -4981919897325933228L;

    public RecoverableException(String message, Throwable cause) {
        super(message, cause);
    }

}
