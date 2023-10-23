package com.university.dbmsibackend.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class Attribute {
    private String name;
    private String type;
    private Integer length;
    private Boolean isNull;
}
