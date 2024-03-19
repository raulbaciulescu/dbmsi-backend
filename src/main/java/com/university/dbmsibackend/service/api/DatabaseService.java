package com.university.dbmsibackend.service.api;

import com.university.dbmsibackend.domain.Database;
import com.university.dbmsibackend.dto.CreateDatabaseRequest;
import com.university.dbmsibackend.exception.EntityAlreadyExistsException;

import java.util.List;

public interface DatabaseService {
    void createDatabase(CreateDatabaseRequest request) throws EntityAlreadyExistsException;

    void dropDatabase(String databaseName);

    List<Database> getDatabases();
}
