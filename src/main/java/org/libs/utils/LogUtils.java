package org.libs.utils;

import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;

import java.nio.file.Path;

public final class LogUtils {

    private LogUtils() {
        throw new UnsupportedOperationException("Lớp tiện ích không thể được khởi tạo.");
    }

    private static String extractCodeLocation(Exception e) {
        StackTraceElement[] stackTrace = e.getStackTrace();
        StackTraceElement ste = stackTrace.length > 0 ? stackTrace[0] : null;

        return (ste != null) ?
                String.format("File: %s, Line: %d", ste.getFileName(), ste.getLineNumber()) :
                "Không xác định (Stack Trace trống)";
    }

    public static void logProcessingError(Logger logger, Exception e, String customMessage) {

        String codeLocation = extractCodeLocation(e);

        String errorMessage = String.format("%s (Lỗi CODE tại: %s)",
                customMessage,
                codeLocation);

        logger.error(errorMessage, e);
    }
}