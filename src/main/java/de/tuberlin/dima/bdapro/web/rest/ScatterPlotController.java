package de.tuberlin.dima.bdapro.web.rest;


import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.http.HttpServletResponse;

import de.tuberlin.dima.bdapro.data.StreamProcessor;
import de.tuberlin.dima.bdapro.error.ErrorType;
import de.tuberlin.dima.bdapro.error.ErrorTypeException;
import de.tuberlin.dima.bdapro.model.ClusterCenter;
import de.tuberlin.dima.bdapro.model.ExecutionType;
import de.tuberlin.dima.bdapro.model.Point;
import de.tuberlin.dima.bdapro.service.DataService;
import de.tuberlin.dima.bdapro.service.MessagingService;
import de.tuberlin.dima.bdapro.util.DataTransformer;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
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
		
		final int[][] dataGrid = dataService.scatterPlot(ExecutionType.SEQUENTIAL,bounds.x, bounds.y);
		return DataTransformer.gridToCoordinates(dataGrid);
	}
	
	
	@GetMapping(value = "/scatter/stream")
	public void scatterPlot(@ModelAttribute("bounds") DimensionalityBounds bounds,
			HttpServletResponse response) {
		dataService.clusterAsync(ExecutionType.KMEANSVDDA, bounds.x, bounds.y, 5, 5, Time.milliseconds(100), Time.milliseconds(100));
	}
	
	
	//NOT WORKING YET!
	@GetMapping(value = "/scatter/clusterstream")
	public Object[] scatterPlot(@ModelAttribute("bounds") DimensionalityBounds bounds, int k, int maxIter,
			HttpServletResponse response) {
		
		DataStream<Tuple4<LocalDateTime, Double, Point, Integer>> points;
		DataStream<Tuple2<Point, ClusterCenter>> clusteredPoints;
/*
        try (OutputStream out = response.getOutputStream()) {
            points = dataService.streamingScatterPlot(bounds.x, bounds.y);
            points.writeToSocket("visualisation-pipeline-service", 8082, new SerializationSchema<Point>() {
                @Override
                public byte[] serialize(Point point) {
                    return ByteBuffer.allocate(4).putDouble(point.getFields()[0]).array();
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(ExceptionUtils.getMessage(e), e);
        }
*/
		List<Double> list = new ArrayList<Double>();
		list.add(5.9);
		list.add(6.0);
		
		return list.toArray();
		
	}
	
	
	@GetMapping(value = "/scatter/stream/start")
	public void writeToQueue() {
		messagingService.produceRandomMessages();
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
