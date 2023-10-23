package com.university.dbmsibackend.domain;

import java.util.List;

public class ForeignKey {
    private List<Attribute> attributes;
    private String referenceTable;
    private List<Attribute> referenceAttributes;
}
