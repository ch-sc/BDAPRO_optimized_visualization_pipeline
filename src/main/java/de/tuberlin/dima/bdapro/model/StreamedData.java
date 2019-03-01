package de.tuberlin.dima.bdapro.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
@AllArgsConstructor
public class StreamedData {

    List<Object[]> data = new ArrayList<>();

}
