package com.university.dbmsibackend.domain;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
public class Index {
    private String name;
    private Boolean isUnique;
    private String type;
}
