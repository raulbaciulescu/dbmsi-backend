package com.university.dbmsibackend.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class Catalog {
    private List<Database> databases;

    public Catalog() {
        databases = new ArrayList<>();
    }
}
