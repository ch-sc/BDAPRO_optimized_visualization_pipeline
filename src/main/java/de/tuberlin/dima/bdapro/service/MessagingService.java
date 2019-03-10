package de.tuberlin.dima.bdapro.service;

import java.util.Arrays;
import java.util.Random;

import de.tuberlin.dima.bdapro.model.dto.TwoDimensionalPlotSto;
import de.tuberlin.dima.bdapro.util.DataTransformer;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
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
	private static final String ROUTING_KEY_BASE = "plot.2d";
	public static final String CLUSTER_DATAPOINT = "CLUSTER_DATAPOINT";
	
	Random random = new Random(101);
	
	
	public void send(String key, Object[] data) {
		String routingKey = ROUTING_KEY_BASE + (Strings.isNullOrEmpty(key) ? "" : '.' + key);
		rabbitTemplate.convertAndSend(EXCHANGE, routingKey, data);
	}
	
	
	@Scheduled(fixedRate = 500)
	public void produceRandomMessages() {
		TwoDimensionalPlotSto plot2d = new TwoDimensionalPlotSto();
		plot2d.setData(new int[100][100]);
		randomDataGrid(plot2d);
		
		Object[] payload = DataTransformer.gridToCoordinates(plot2d.getData());
		
		log.info("pushing data to queue...");
		if (log.isDebugEnabled()) {
			log.debug("message body: " + Arrays.deepToString(payload));
		}
		
		MessageProperties props = new MessageProperties();
		props.setContentType(MediaType.TEXT_PLAIN_VALUE);
		
		rabbitTemplate.convertAndSend(EXCHANGE, ROUTING_KEY_BASE, payload);
	}
	
	
	private void randomDataGrid(TwoDimensionalPlotSto plot) {
		int[][] data = plot.getData();
		int[] row;
		for (int r = 0; r < data.length; r++) {
			row = data[r];
			for (int c = 0; c < row.length; c++) {
				// only 1/4 of the data point will be set
				data[r][c] = random.nextBoolean() && random.nextBoolean() ? random.nextInt(1_000) : 0;
			}
		}
	}
	
}
