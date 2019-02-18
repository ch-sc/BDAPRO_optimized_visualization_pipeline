package de.tuberlin.dima.bdapro.web.rest;


import java.io.IOException;
import java.io.OutputStream;
import javax.servlet.http.HttpServletResponse;

import de.tuberlin.dima.bdapro.data.StreamedDataProcessor;
import de.tuberlin.dima.bdapro.error.ErrorType;
import de.tuberlin.dima.bdapro.error.ErrorTypeException;
import de.tuberlin.dima.bdapro.service.DataService;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/2d")
public class ScatterPlotController {
	
	private final DataService dataService;
	private final StreamedDataProcessor streamedDataProcessor;
	
	
	@Autowired
	public ScatterPlotController(DataService dataService, StreamedDataProcessor streamedDataProcessor) {
		this.dataService = dataService;
		this.streamedDataProcessor = streamedDataProcessor;
	}
	
	
	@GetMapping(value = "/scatter")
	public int[][] scatterPlot(@RequestParam(value = "x", required = false, defaultValue = "1000") Integer x,
			@RequestParam(value = "y", required = false, defaultValue = "1000") Integer y) {
		
		if (x == null && y == null) {
			return dataService.scatterPlot();
		} else if (x == null) {
			x = y;
		} else if (y == null) {
			y = x;
		} else if (x <= 0 || y <= 0) {
			throw new ErrorTypeException(ErrorType.PARAMETER_ERROR, "Only positive values are allowed.");
		}
		
		return dataService.scatterPlot(x, y);
	}
	
	
	@GetMapping(value = "/scatter/stream")
	public void scatterPlot(@RequestParam(value = "x", required = false, defaultValue = "1000") Integer xDim,
			@RequestParam(value = "y", required = false, defaultValue = "1000") Integer yDim,
			HttpServletResponse response) {
		
		try (OutputStream out = response.getOutputStream()) {
			streamedDataProcessor.streamedScatterPlot(xDim, yDim, out);
		} catch (IOException e) {
			throw new RuntimeException(ExceptionUtils.getMessage(e), e);
		}
	}
	
	
}
