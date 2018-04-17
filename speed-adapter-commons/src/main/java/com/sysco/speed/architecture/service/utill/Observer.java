package com.sysco.speed.architecture.service.utill;

import com.sysco.speed.architecture.service.ConsumerResult;

/**
 * Created by charithap on 9/27/17.
 * The Listener interface that allows users implement and apply their logic to execute when data is received from the stream
 */
public interface Observer {

    void onDataReceived (ConsumerResult data);
}
