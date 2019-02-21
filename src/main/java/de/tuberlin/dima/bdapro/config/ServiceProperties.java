package de.tuberlin.dima.bdapro.config;


import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(value = "service", ignoreUnknownFields = true)
@Getter
@Setter
@ToString
public class ServiceProperties {
	
	private DataConfig data = new DataConfig();
	private FlinkConfig flink = new FlinkConfig();
	
	@Getter
	@Setter
	@ToString
	static class FlinkConfig {
		
		String[] args;
	}
}
