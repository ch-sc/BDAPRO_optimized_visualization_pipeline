package de.tuberlin.dima.bdapro.web.rest;


import java.io.IOException;
import java.io.OutputStream;
import javax.servlet.http.HttpServletResponse;
import javax.websocket.server.PathParam;

import de.tuberlin.dima.bdapro.data.taxi.StreamDataProcessor;
import de.tuberlin.dima.bdapro.error.ErrorType;
import de.tuberlin.dima.bdapro.error.ErrorTypeException;
import de.tuberlin.dima.bdapro.model.ExecutionType;
import de.tuberlin.dima.bdapro.service.DataService;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/2d")
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
	
	
	@GetMapping(value = "/scatter")
	public int[][] scatterPlot(@ModelAttribute("bounds") DimensionalityBounds bounds) throws ErrorTypeException {
		if (bounds.isUnbound()) {
			return dataService.scatterPlot();
		}
		return dataService.scatterPlot(bounds.x, bounds.y);
	}
	
	
	@GetMapping(value = "/scatter/stream")
	public void scatterPlot(@ModelAttribute("bounds") DimensionalityBounds bounds,
			HttpServletResponse response) {
		
		try (OutputStream out = response.getOutputStream()) {
			dataService.scatterPlot(bounds.x, bounds.y, out);
		} catch (IOException e) {
			throw new RuntimeException(ExceptionUtils.getMessage(e), e);
		}
	}
	
	
	static class DimensionalityBounds {
		
		private Integer x;
		private Integer y;
		
		
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
