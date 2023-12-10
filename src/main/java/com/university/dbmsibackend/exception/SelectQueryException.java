package com.university.dbmsibackend.exception;


public class SelectQueryException extends RuntimeException {
    public SelectQueryException(String msg) {
        super(msg);
    }
}
