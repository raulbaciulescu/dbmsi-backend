package com.university.dbmsibackend.controller;


import com.university.dbmsibackend.dto.CreateTableRequest;
import com.university.dbmsibackend.dto.InsertRequest;
import com.university.dbmsibackend.dto.SelectAllResponse;
import com.university.dbmsibackend.service.TableService;
import lombok.AllArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@AllArgsConstructor
@RequestMapping(("/tables"))
@CrossOrigin(origins = "http://localhost:3000")
public class TableController {
    private TableService service;

    @PostMapping
    @ResponseStatus(HttpStatus.OK)
    public void createTable(@RequestBody CreateTableRequest request) {
        service.createTable(request);
    }

    @DeleteMapping("/{databaseName}/{tableName}")
    @CrossOrigin(origins = "http://localhost:3000")
    @ResponseStatus(HttpStatus.OK)
    public void dropTable(@PathVariable String databaseName, @PathVariable String tableName) {
        service.dropTable(databaseName, tableName);
    }

    @PostMapping("/insert")
    @ResponseStatus(HttpStatus.OK)
    public void insertRow(@RequestBody InsertRequest request) {
        service.insertRow(request);
    }

    @GetMapping("/{databaseName}/{tableName}")
    @ResponseStatus(HttpStatus.OK)
    public List<SelectAllResponse> selectAll(@PathVariable String databaseName, @PathVariable String tableName) {
        return service.selectAll(databaseName, tableName);
    }

    @DeleteMapping("/delete-row/{databaseName}/{tableName}")
    @ResponseStatus(HttpStatus.OK)
    public void deleteRow(
            @PathVariable String databaseName,
            @PathVariable String tableName,
            @RequestBody List<String> primaryKeys) {
        service.deleteRow(databaseName, tableName, primaryKeys);
    }
}
