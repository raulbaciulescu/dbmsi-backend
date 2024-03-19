package com.university.dbmsibackend.controller;

import com.university.dbmsibackend.dto.CreateForeignKeyRequest;
import com.university.dbmsibackend.service.api.ForeignKeyService;
import lombok.AllArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@AllArgsConstructor
@RequestMapping(("/constraints"))
@CrossOrigin(origins = "http://localhost:3000")
public class ForeignKeyController {
    private ForeignKeyService service;

    @PostMapping
    @ResponseStatus(HttpStatus.OK)
    public void createForeignKey(@RequestBody CreateForeignKeyRequest request) {
        service.createForeignKey(request);
    }
}
