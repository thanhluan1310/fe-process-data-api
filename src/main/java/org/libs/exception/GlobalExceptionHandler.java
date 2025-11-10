package org.libs.exception;


import org.apache.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.ServletWebRequest;
import org.springframework.web.context.request.WebRequest;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.LocalDateTime;

@ControllerAdvice
public class GlobalExceptionHandler {
    private static final Logger logger = Logger.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleAllExceptions(Exception ex, WebRequest request) {
        logger.error("Lỗi không mong muốn xảy ra khi xử lý yêu cầu tại đường dẫn: " +
                        ((ServletWebRequest) request).getRequest().getRequestURI(),
                ex);

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        ex.printStackTrace(pw);
        String stackTrace = sw.toString().substring(0, 500);

        String path = ((ServletWebRequest) request).getRequest().getRequestURI();

        String errorMessage = getRootMessage(ex);
        ErrorResponse errorResponse = new ErrorResponse(
                LocalDateTime.now().toString(),
                HttpStatus.INTERNAL_SERVER_ERROR.value(),
                HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase(),
                stackTrace,
                errorMessage,
                path
        );

        return new ResponseEntity<>(errorResponse, HttpStatus.INTERNAL_SERVER_ERROR);
    }

    private String getRootMessage(Throwable ex) {
        Throwable rootCause = ex.getCause() != null ? ex.getCause() : ex;
        return rootCause.getMessage() != null ? rootCause.getMessage() : "An unexpected error occurred.";
    }

    public record ErrorResponse(
            String timestamp,
            int status,
            String error,
            String trace,
            String message,
            String path
    ) {
    }
}
