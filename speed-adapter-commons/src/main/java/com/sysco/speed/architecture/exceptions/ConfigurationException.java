package com.sysco.speed.architecture.exceptions;

/**
 * Created by nandunin on 9/27/17.
 */
public class ConfigurationException extends Exception {

    public ConfigurationException() {
    }

    public ConfigurationException(String var1) {
        super(var1);
    }

    public ConfigurationException(String var1, Throwable var2) {
        super(var1, var2);
    }

    public ConfigurationException(Throwable var1) {
        super(var1);
    }
}
