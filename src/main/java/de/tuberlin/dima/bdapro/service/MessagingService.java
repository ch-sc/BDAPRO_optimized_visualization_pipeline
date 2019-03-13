package de.tuberlin.dima.bdapro.service;

import java.util.Arrays;
import java.util.Random;

import de.tuberlin.dima.bdapro.model.dto.TwoDimensionalPlotSto;
import de.tuberlin.dima.bdapro.util.DataTransformer;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;

@Log4j2
@Service
public class MessagingService {
	
	private final RabbitTemplate rabbitTemplate;
	
	private static final String EXCHANGE = "BDAPRO";
	private static final String ROUTING_KEY_BASE = "plot.2d";
	
	private static final Random random = new Random(101);
	
	
	@Autowired
	public MessagingService(RabbitTemplate rabbitTemplate) {
		this.rabbitTemplate = rabbitTemplate;
	}
	
	
	/**
	 * sends a message to the RabbitMQ message broker.
	 *
	 * @param key key of the message
	 * @param data body of the message
	 */
	public void send(String key, Object[] data) {
		String routingKey = ROUTING_KEY_BASE + (StringUtils.isBlank(key) ? "" : '.' + key);
		rabbitTemplate.convertAndSend(EXCHANGE, routingKey, data);
	}
	
	
	/**
	 * creates random data grids of size 100x100, with a filling rate of about 25% and sends its data points as messages
	 * to the RabbitMQ message broker.
	 *
	 * @param amount amount of generated data grids
	 */
	//	@Scheduled(fixedRate = 500)
	public void sendRandom(int amount) {
		
		log.info("push " + amount + " random data messages to queue...");
		
		for (int i = 0; i < amount; i++) {
			
			TwoDimensionalPlotSto plot2d = new TwoDimensionalPlotSto();
			plot2d.setData(new int[100][100]);
			createDataGrid(plot2d);
			
			Object[] payload = DataTransformer.gridToCoordinates(plot2d.getData());
			
			if (log.isDebugEnabled()) {
				log.debug("message body: " + Arrays.deepToString(payload));
			}
			
			MessageProperties props = new MessageProperties();
			props.setContentType(MediaType.TEXT_PLAIN_VALUE);
			
			rabbitTemplate.convertAndSend(EXCHANGE, ROUTING_KEY_BASE + ".random", payload);
			
		}
	}
	
	
	private void createDataGrid(TwoDimensionalPlotSto plot) {
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
