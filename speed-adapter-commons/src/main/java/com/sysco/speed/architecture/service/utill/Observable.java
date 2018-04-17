package com.sysco.speed.architecture.service.utill;


import com.sysco.speed.architecture.service.ConsumerResult;

/**
 * Created by charithap on 9/27/17.
 */
public interface Observable {
    void notifyObserver (ConsumerResult data);
}
