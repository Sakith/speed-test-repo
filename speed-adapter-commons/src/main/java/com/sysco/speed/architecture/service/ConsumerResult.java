package com.sysco.speed.architecture.service;

/**
 * Created by charithap on 9/28/17.
 */
public class ConsumerResult {

    private String data;

    private String sequenceNumber;

    private String partitionKey;

    public ConsumerResult(String sequenceNumber, String partitionKey, String data) {
        this.data = data;
        this.partitionKey = partitionKey;
        this.sequenceNumber = sequenceNumber;
    }

    public String getData() {
        return data;
    }

    public String getSequenceNumber() {
        return sequenceNumber;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

}
