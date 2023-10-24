package com.university.dbmsibackend.domain;

import lombok.*;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
public class ForeignKey {
    private String name;
    private List<Attribute> attributes;
    private String referenceTable;
    private List<Attribute> referenceAttributes;
}
