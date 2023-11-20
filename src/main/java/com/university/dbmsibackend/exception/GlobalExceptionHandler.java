package com.university.dbmsibackend.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;

import java.util.Date;

@ControllerAdvice
public class GlobalExceptionHandler {
    @ExceptionHandler(EntityNotFoundException.class)
    public ResponseEntity<?> handleEntityNotFoundException(EntityNotFoundException exception, WebRequest request) {
        ErrorDetails errorDetails =
                new ErrorDetails(new Date(), "entity-not-found-exception", exception.getMessage(), String.valueOf(HttpStatus.NOT_FOUND.value()));

        return new ResponseEntity<>(errorDetails, HttpStatus.NOT_FOUND);
    }

    @ExceptionHandler(EntityAlreadyExistsException.class)
    public ResponseEntity<?> handleEntityAlreadyExistsException(EntityAlreadyExistsException exception, WebRequest request) {
        ErrorDetails errorDetails =
                new ErrorDetails(new Date(), "entity-already-exist", exception.getMessage(), String.valueOf(HttpStatus.BAD_REQUEST.value()));

        return new ResponseEntity<>(errorDetails, HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(ForeignKeyViolationException.class)
    public ResponseEntity<?> handleForeignKeyViolationException(ForeignKeyViolationException exception, WebRequest request) {
        ErrorDetails errorDetails =
                new ErrorDetails(new Date(), "foreign-key-exception", exception.getMessage(), String.valueOf(HttpStatus.BAD_REQUEST.value()));

        return new ResponseEntity<>(errorDetails, HttpStatus.BAD_REQUEST);
    }
}
