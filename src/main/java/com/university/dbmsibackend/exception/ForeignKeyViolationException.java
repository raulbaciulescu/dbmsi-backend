package com.university.dbmsibackend.exception;


public class ForeignKeyViolationException extends RuntimeException {
    public ForeignKeyViolationException(String msg) {
        super(msg);
    }
}
