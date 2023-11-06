package com.university.dbmsibackend.exception;

import java.util.Date;


public record ErrorDetails(Date timestamp, String errorCode, String message, String status) {
}
