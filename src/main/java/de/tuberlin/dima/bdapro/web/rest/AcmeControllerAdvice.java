package de.tuberlin.dima.bdapro.web.rest;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.OffsetDateTime;
import javax.servlet.http.HttpServletRequest;

import de.tuberlin.dima.bdapro.error.CustomError;
import de.tuberlin.dima.bdapro.error.ErrorType;
import de.tuberlin.dima.bdapro.error.ErrorTypeException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@RestControllerAdvice
@Slf4j
public class AcmeControllerAdvice extends ResponseEntityExceptionHandler {
	
	@ExceptionHandler(Throwable.class)
	@ResponseBody
	ResponseEntity<?> handleControllerException(HttpServletRequest request, Throwable ex) {
		final CustomError.CustomErrorBuilder builder = CustomError.builder()
				.error(ExceptionUtils.getMessage(ex))
				.rootMessage(ExceptionUtils.getRootCauseMessage(ex))
				.timestamp(OffsetDateTime.now());
		
		int status = getStatus(request).value();
		
		if (ex instanceof ErrorTypeException) {
			ErrorType type = getType((ErrorTypeException) ex);
			status = type.getStatus();
			builder.type(type);
		}
		
		builder.status(status);
		
		final CustomError error = builder.uri(getRequestUri(request))
				.trace(getStackTrace(ex))
				.build();
		
		if (status >= 500) {
			log.error(error.getError(), ex);
		}
		
		return new ResponseEntity<>(error, HttpStatus.valueOf(status));
	}
	
	
	private HttpStatus getStatus(HttpServletRequest request) {
		Integer statusCode = (Integer) request.getAttribute("javax.servlet.error.status_code");
		if (statusCode == null) {
			return HttpStatus.INTERNAL_SERVER_ERROR;
		}
		return HttpStatus.valueOf(statusCode);
	}
	
	
	public String getRequestUri(HttpServletRequest req) {
		String requestUri = (String) req.getAttribute("javax.servlet.error.request_uri");
		return StringUtils.isNotBlank(requestUri) ? requestUri : req.getRequestURI();
	}
	
	
	public String getStackTrace(Throwable throwable) {
		StringWriter stackTraceWriter = new StringWriter();
		throwable.printStackTrace(new PrintWriter(stackTraceWriter));
		stackTraceWriter.flush();
		return stackTraceWriter.toString();
	}
	
	
	public ErrorType getType(ErrorTypeException exception) {
		return exception.getErrorType();
	}
}
