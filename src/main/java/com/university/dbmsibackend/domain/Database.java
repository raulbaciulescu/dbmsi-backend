package com.university.dbmsibackend.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;


@Getter
@Setter
public class Database {
    private String name;
    private List<Table> tables;

    public Database(String name) {
        this.name = name;
        tables = new ArrayList<>();
    }
}
