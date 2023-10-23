package com.university.dbmsibackend.controller;


import com.university.dbmsibackend.dto.CreateTableRequest;
import com.university.dbmsibackend.service.TableService;
import lombok.AllArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

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
}
