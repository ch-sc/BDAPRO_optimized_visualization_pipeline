package de.tuberlin.dima.bdapro.service;

import java.util.Arrays;
import java.util.Random;

import de.tuberlin.dima.bdapro.model.dto.TwoDimensionalPlotSto;
import lombok.extern.log4j.Log4j2;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Log4j2
@Service
public class MessagingService {
	
	@Autowired
	private RabbitTemplate rabbitTemplate;
	
	private static final String EXCHANGE = "BDAPRO";
	private static final String ROUTING_KEY = "plot.2d";
	
	Random random = new Random(101);
	
	
	@Scheduled(fixedRate = 1000)
	public void produce() {
		TwoDimensionalPlotSto plot2d = new TwoDimensionalPlotSto();
		plot2d.setData(new int[100][100]);
		createData(plot2d);
		
		if (log.isDebugEnabled()) {
			log.debug("push data to queue: " + Arrays.deepToString(plot2d.getData()));
		}
		
		MessageProperties props = new MessageProperties();
		props.setContentType(MediaType.TEXT_PLAIN_VALUE);
		rabbitTemplate.convertAndSend(EXCHANGE, ROUTING_KEY, plot2d.getData());
	}
	
	
	private void createData(TwoDimensionalPlotSto plot) {
		int[][] data = plot.getData();
		for (int rowIndex = 0; rowIndex < data.length; rowIndex++) {
			int[] row = data[rowIndex];
			for (int column : row) {
				data[rowIndex][column] = random.nextInt(1_000_000);
			}
		}
	}
	
}
