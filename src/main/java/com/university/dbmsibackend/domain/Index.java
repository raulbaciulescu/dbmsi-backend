package com.university.dbmsibackend.domain;

import lombok.*;

import java.util.List;

@NoArgsConstructor
@AllArgsConstructor
@Builder
@Getter
@Setter
public class Index {
    private String name;
    private Boolean isUnique;
    private String type;
    private List<Attribute> attributes;
}
