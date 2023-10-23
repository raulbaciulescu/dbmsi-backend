package com.university.dbmsibackend;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@AllArgsConstructor
@ToString
@Getter
@Setter
public class Foo {
    Integer number;
    String text;
    List<Integer> numbers;
}
