package de.tuberlin.dima.bdapro.error;

import java.time.OffsetDateTime;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class CustomError {
	
	private int status;
	private String error;
	private String rootMessage;
	private OffsetDateTime timestamp;
	private String uri;
	private String trace;
	private ErrorType type;
	
}
