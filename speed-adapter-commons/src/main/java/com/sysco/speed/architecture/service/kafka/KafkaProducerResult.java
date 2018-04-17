package com.sysco.speed.architecture.service.kafka;

import com.sysco.speed.architecture.service.ProducerResult;

/**
 * Created by charithap on 9/18/17.
 */
public class KafkaProducerResult extends ProducerResult {
    private String partitionId;

    public KafkaProducerResult(String sequenceId, String partitionId) {
        super(sequenceId);
        this.partitionId = partitionId;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(String partitionId) {
        this.partitionId = partitionId;
    }
}
