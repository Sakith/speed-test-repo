package com.sysco.speed.architecture.service.kinesis;

import com.sysco.speed.architecture.service.ProducerResult;

/**
 * Created by charithap on 9/18/17.
 */
public class KinesisProducerResult extends ProducerResult {

    private String shardId;

    public KinesisProducerResult(String sequenceId, String shardId) {
        super(sequenceId);
        this.shardId = shardId;
    }

    public String getShardId() {
        return shardId;
    }

    public void setShardId(String shardId) {
        this.shardId = shardId;
    }
}
