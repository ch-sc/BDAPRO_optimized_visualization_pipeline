package de.tuberlin.dima.bdapro.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class AppConfig {

	@JsonProperty("data-location")
	private String dataLocation;
	

}
