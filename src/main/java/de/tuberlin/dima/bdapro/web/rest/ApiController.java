package de.tuberlin.dima.bdapro.web.rest;


import de.tuberlin.dima.bdapro.service.DataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class ApiController {
	
	@Autowired
	private DataService dataService;
	
	@GetMapping(value = "scatter")
	public int[][] scatterPlot (@RequestParam(value = "x", required = false, defaultValue = "1000") Integer xDimension,
			@RequestParam(value = "y", required = false, defaultValue = "1000") Integer yDimension) {
		return dataService.scatterPlot(xDimension,yDimension);
	}
}
