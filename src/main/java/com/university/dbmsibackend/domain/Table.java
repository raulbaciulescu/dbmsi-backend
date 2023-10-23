package com.university.dbmsibackend.domain;

import lombok.*;

import java.util.List;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@Builder
public class Table {
    private String name;
    private String fileName;
    private List<Attribute> attributes;
    private List<String> primaryKeys;
    private List<ForeignKey> foreignKeys;
    private List<Index> indexes;
}
