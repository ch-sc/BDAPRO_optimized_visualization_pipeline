package de.tuberlin.dima.bdapro.error;

import lombok.Getter;

public class ErrorTypeException extends RuntimeException {
	
	@Getter private final ErrorType errorType;
	
	public ErrorTypeException(ErrorType errorType) {
		this.errorType = errorType;
	}
	
	
	public ErrorTypeException(ErrorType errorType, String message) {
		super(message);
		this.errorType = errorType;
	}
	
	
	public ErrorTypeException(ErrorType errorType, String message, Throwable cause) {
		super(message, cause);
		this.errorType = errorType;
	}
	
	
	public ErrorTypeException(ErrorType errorType, Throwable cause) {
		super(cause);
		this.errorType = errorType;
	}
	
	
	public ErrorTypeException(ErrorType errorType, String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		this.errorType = errorType;
	}
}
