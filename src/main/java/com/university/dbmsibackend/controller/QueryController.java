package com.university.dbmsibackend.controller;

import com.university.dbmsibackend.dto.QueryRequest;
import com.university.dbmsibackend.service.QueryService;
import com.university.dbmsibackend.service.QueryService2;
import lombok.AllArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@AllArgsConstructor
@RequestMapping(("/sqlCommand"))
@CrossOrigin(origins = "http://localhost:3000")
public class QueryController {
    private QueryService2 service;
    
    @PostMapping
    @ResponseStatus(HttpStatus.OK)
    public List<Map<String, String>> query(@RequestBody QueryRequest request){
        return service.executeQuery(request);
    }

}
