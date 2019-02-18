package de.tuberlin.dima.bdapro.config;


import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(value = "service", ignoreUnknownFields = true)
@Data
public class ServiceProperties {
	
	private DataConfig data = new DataConfig();
	
}
