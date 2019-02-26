package de.tuberlin.dima.bdapro.web.rest;


import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import javax.websocket.server.PathParam;

import de.tuberlin.dima.bdapro.data.taxi.StreamDataProcessor;
import de.tuberlin.dima.bdapro.error.ErrorType;
import de.tuberlin.dima.bdapro.error.ErrorTypeException;
import de.tuberlin.dima.bdapro.model.ExecutionType;
import de.tuberlin.dima.bdapro.service.DataService;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.java.Log;
import lombok.extern.log4j.Log4j;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.runtime.util.IntArrayList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/2d")
@Log4j2
public class ScatterPlotController {
	
	private final DataService dataService;
	private final StreamDataProcessor streamDataProcessor;
	
	
	@Autowired
	public ScatterPlotController(DataService dataService, StreamDataProcessor streamDataProcessor) {
		this.dataService = dataService;
		this.streamDataProcessor = streamDataProcessor;
	}
	
	
	@ModelAttribute("bounds")
	public DimensionalityBounds parameterPreProcessing(@RequestParam(value = "x", required = false) Integer x,
			@RequestParam(value = "y", required = false) Integer y) throws ErrorTypeException {
		if ((x != null && x <= 0) || (y != null && y <= 0)) {
			throw new ErrorTypeException(ErrorType.PARAMETER_ERROR, "Only positive values are allowed.");
		}
		
		return new DimensionalityBounds(x, y);
	}
	
	
	@GetMapping(value = "/scatter", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
	public Object[] scatterPlot(@ModelAttribute("bounds") DimensionalityBounds bounds) throws ErrorTypeException {
		if (bounds.isUnbound()) {
			return dataService.scatterPlot();
		}
		
		final int[][] dataGrid = dataService.scatterPlot(bounds.x, bounds.y);
		return convert(dataGrid);
	}
	
	
	@GetMapping(value = "/scatter/stream")
	public void scatterPlot(@ModelAttribute("bounds") DimensionalityBounds bounds,
			HttpServletResponse response) {
		
		try (OutputStream out = response.getOutputStream()) {
			dataService.scatterPlot(bounds.x, bounds.y);
		} catch (IOException e) {
			throw new RuntimeException(ExceptionUtils.getMessage(e), e);
		}
	}
	
	
	private Object[] convert(int[][] grid) {
		List<int[]> list = new ArrayList<>(grid.length);
		
		for (int i = 0; i < grid.length; i++) {
			final int[] row = grid[i];
			for (int j = 0; j < row.length; j++) {
				if (row[j] == 0) {
					continue;
				}
				list.add(new int[] { i, j, row[j] });
			}
		}
		return list.toArray();
	}
	
	
	static class DimensionalityBounds {
		
		final private Integer x;
		final private Integer y;
		
		
		DimensionalityBounds(Integer x, Integer y) {
			if (y == null) {
				y = x;
			} else if (x == null) {
				x = y;
			}
			this.x = x;
			this.y = y;
		}
		
		
		boolean isUnbound() {
			return x == null && y == null;
		}
	}
}
