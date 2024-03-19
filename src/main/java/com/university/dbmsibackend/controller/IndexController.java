package com.university.dbmsibackend.controller;

import com.university.dbmsibackend.dto.CreateIndexRequest;
import com.university.dbmsibackend.service.api.IndexService;
import lombok.AllArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@AllArgsConstructor
@RequestMapping(("/indexes"))
@CrossOrigin(origins = "http://localhost:3000")
public class IndexController {
    private IndexService service;

    @PostMapping
    @ResponseStatus(HttpStatus.OK)
    public void createIndex(@RequestBody CreateIndexRequest request) {
        service.createIndex(request);
    }
}
