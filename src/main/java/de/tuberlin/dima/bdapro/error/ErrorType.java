package de.tuberlin.dima.bdapro.error;

import lombok.Getter;

@Getter
public enum ErrorType {
	PARAMETER_ERROR(406);
	
	private int status;
	
	ErrorType(int status) {
		this.status = status;
	}
	
}
