package com.sysco.speed.architecture.exceptions;

/**
 * Created by nandunin on 9/15/17.
 */
public class SchemaException extends RuntimeException {

    public SchemaException() {
    }

    public SchemaException(String var1) {
        super(var1);
    }

    public SchemaException(String var1, Throwable var2) {
        super(var1, var2);
    }

    public SchemaException(Throwable var1) {
        super(var1);
    }
}
