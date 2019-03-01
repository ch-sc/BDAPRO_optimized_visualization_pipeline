package de.tuberlin.dima.bdapro.data.dto;

import lombok.Data;

@Data
public class ScatterPlotMessage {

    private int xDimension;
    private int yDimension;

    private boolean initiateProcessing;
    private boolean stopProcessing;


}
