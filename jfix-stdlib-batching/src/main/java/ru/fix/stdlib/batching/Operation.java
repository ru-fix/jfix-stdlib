package ru.fix.stdlib.batching;

/**
 * @author Kamil Asfandiyarov
 */
class Operation<PayloadT> {

    private final long creationTimestamp;
    private final PayloadT payload;

    public Operation(PayloadT payload, long creationTimestamp) {
        this.creationTimestamp = creationTimestamp;
        this.payload = payload;
    }

    public long getCreationTimestamp() {
        return creationTimestamp;
    }

    public PayloadT getPayload() {
        return payload;
    }
}
