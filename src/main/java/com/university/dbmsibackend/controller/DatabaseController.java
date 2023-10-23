package com.university.dbmsibackend.controller;

import com.university.dbmsibackend.domain.Database;
import com.university.dbmsibackend.dto.CreateDatabaseRequest;
import com.university.dbmsibackend.service.DatabaseService;
import lombok.AllArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@AllArgsConstructor
@RequestMapping(("/databases"))
public class DatabaseController {
    private DatabaseService service;

    @PostMapping
    @ResponseStatus(HttpStatus.OK)
    public void createDatabase(@RequestBody CreateDatabaseRequest request) {
        service.createDatabase(request);
    }

    @GetMapping
    public List<Database> getDatabases() {
        return service.getDatabases();
    }
}
