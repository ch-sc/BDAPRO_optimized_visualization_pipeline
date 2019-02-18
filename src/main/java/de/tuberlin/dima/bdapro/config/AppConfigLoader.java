package de.tuberlin.dima.bdapro.config;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import de.tuberlin.dima.bdapro.error.BusinessException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

@Slf4j
public class AppConfigLoader {
	
	
	private static final String CONFIG_FILE = "application.yml";
	
	@Getter
	private static String configLocation = null;
	
	
	public static DataConfig load(String configLocationPath) {
		
		configLocation = configLocationPath == null ? "src/main/resources/" : configLocationPath;
		
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		
		try {
			DataConfig config = mapper.readValue(new File(configLocation, CONFIG_FILE), DataConfig.class);
			
			log.info(config.toString()/* ReflectionToStringBuilder.toString(config, ToStringStyle.MULTI_LINE_STYLE)*/);
			
			return config;
		} catch (IOException e) {
			throw new BusinessException("Could not read configuration: " + ExceptionUtils.getMessage(e), e);
		}
	}
	
	
}
