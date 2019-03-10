package de.tuberlin.dima.bdapro.web.rest;


import de.tuberlin.dima.bdapro.data.StreamProcessor;
import de.tuberlin.dima.bdapro.error.ErrorType;
import de.tuberlin.dima.bdapro.error.ErrorTypeException;
import de.tuberlin.dima.bdapro.model.ExecutionType;
import de.tuberlin.dima.bdapro.service.DataService;
import de.tuberlin.dima.bdapro.service.MessagingService;
import de.tuberlin.dima.bdapro.util.DataTransformer;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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
	private final MessagingService messagingService;
	private final StreamProcessor m4StreamProcessor;
	private final StreamProcessor vddaStreamProcessor;
	
	
	@Autowired
	public ScatterPlotController(DataService dataService, MessagingService messagingService,
			@Qualifier("data-processor.simpleStream") StreamProcessor m4StreamProcessor,
			@Qualifier("data-processor.kMeansVDDA") StreamProcessor vddaStreamProcessor) {
		this.dataService = dataService;
		this.messagingService = messagingService;
		this.m4StreamProcessor = m4StreamProcessor;
		this.vddaStreamProcessor = vddaStreamProcessor;
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
			return dataService.scatterPlot(ExecutionType.SEQUENTIAL);
		}
		
		final int[][] dataGrid = dataService.scatterPlot(ExecutionType.SEQUENTIAL, bounds.x, bounds.y);
		return DataTransformer.gridToCoordinates(dataGrid);
	}
	
	
	@GetMapping(value = "/stream/cluster")
	public void createClusterStreamAsync(@ModelAttribute("bounds") DimensionalityBounds bounds,
			@RequestParam(value = "f", required = false, defaultValue = "KMEANSVDDA") ExecutionType executionType) {
		dataService.clusterAsync(executionType, bounds.x, bounds.y, 5, 5, Time.hours(10),
				Time.milliseconds(100));
	}
	
	
	@GetMapping(value = "/stream/scatter")
	public void createScatterStreamAsync(@ModelAttribute("bounds") DimensionalityBounds bounds,
			@RequestParam(value = "f", required = false, defaultValue = "VDDASTREAMING") ExecutionType executionType) {
		dataService.scatterAsync(ExecutionType.KMEANSVDDA, bounds.x, bounds.y, Time.hours(10),
				Time.milliseconds(100));
	}
	
	
	@GetMapping(value = "/scatter/stream/random")
	public void writeToQueue() {
		messagingService.sendRandom(1000);
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
