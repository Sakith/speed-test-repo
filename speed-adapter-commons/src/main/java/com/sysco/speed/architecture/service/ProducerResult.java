package com.sysco.speed.architecture.service;

import com.amazonaws.services.kinesis.model.PutRecordResult;

/**
 * Created by charithap on 9/18/17.
 */
public class ProducerResult{
    private String sequenceId;

    public ProducerResult() {

    }

    public ProducerResult(String sequenceId) {
        this.sequenceId = sequenceId;
    }

    public String getSequenceId() {
        return sequenceId;
    }

    public void setSequenceId(String sequenceId) {
        this.sequenceId = sequenceId;
    }

}
