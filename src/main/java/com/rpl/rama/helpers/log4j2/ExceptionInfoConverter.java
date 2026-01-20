package com.rpl.rama.helpers.log4j2;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;
import org.apache.logging.log4j.core.impl.ThrowableFormatOptions;
import org.apache.logging.log4j.core.pattern.ThrowablePatternConverter;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Custom Log4j2 PatternConverter that handles Clojure ExceptionInfo specially.
 *
 * For clojure.lang.ExceptionInfo, it appends the exception's data map after the message.
 * For all other exceptions, it behaves like the default throwable converter.
 *
 * Usage in log4j2.xml pattern:
 *   %exInfo        - full stack trace (default)
 *   %exInfo{short} - first line only with data
 *   %exInfo{n}     - first n lines of stack trace
 *   %exInfo{none}  - suppress output
 */
@Plugin(name = "ExceptionInfoConverter", category = PatternConverter.CATEGORY)
@ConverterKeys({"exInfo", "exceptionInfo"})
public class ExceptionInfoConverter extends LogEventPatternConverter {

    private static final String EXCEPTION_INFO_CLASS = "clojure.lang.ExceptionInfo";

    private final String[] options;
    private final boolean shortFormat;
    private final boolean none;
    private final int lines;

    private ExceptionInfoConverter(String[] options) {
        super("ExceptionInfo", "exceptionInfo");
        this.options = options;

        if (options == null || options.length == 0) {
            this.shortFormat = false;
            this.none = false;
            this.lines = Integer.MAX_VALUE;
        } else {
            String opt = options[0].toLowerCase();
            this.none = opt.equals("none") || opt.equals("0");
            this.shortFormat = opt.equals("short");

            int parsedLines = Integer.MAX_VALUE;
            if (!none && !shortFormat) {
                try {
                    parsedLines = Integer.parseInt(opt);
                } catch (NumberFormatException e) {
                    // ignore, use default
                }
            }
            this.lines = parsedLines;
        }
    }

    public static ExceptionInfoConverter newInstance(String[] options) {
        return new ExceptionInfoConverter(options);
    }

    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        Throwable throwable = event.getThrown();
        if (throwable == null) {
            return;
        }

        if (none) {
            return;
        }

        if (isExceptionInfo(throwable)) {
            formatExceptionInfo(throwable, toAppendTo);
        } else {
            formatDefault(throwable, toAppendTo);
        }
    }

    private boolean isExceptionInfo(Throwable throwable) {
        // Check by class name to avoid hard dependency on Clojure
        Class<?> clazz = throwable.getClass();
        while (clazz != null) {
            if (EXCEPTION_INFO_CLASS.equals(clazz.getName())) {
                return true;
            }
            clazz = clazz.getSuperclass();
        }
        return false;
    }

    private void formatExceptionInfo(Throwable throwable, StringBuilder toAppendTo) {
        String className = throwable.getClass().getName();
        String message = throwable.getMessage();
        String data = getExceptionInfoData(throwable);

        if (shortFormat) {
            // Short format: ClassName: message {data}
            toAppendTo.append(className);
            if (message != null) {
                toAppendTo.append(": ").append(message);
            }
            if (data != null) {
                toAppendTo.append(" ").append(data);
            }
        } else {
            // Full format: first line with data, then stack trace
            toAppendTo.append(className);
            if (message != null) {
                toAppendTo.append(": ").append(message);
            }
            if (data != null) {
                toAppendTo.append(" ").append(data);
            }
            toAppendTo.append(System.lineSeparator());
            appendStackTrace(throwable, toAppendTo);
        }
    }

    private String getExceptionInfoData(Throwable throwable) {
        try {
            // Use reflection to call getData() on ExceptionInfo
            // This avoids a compile-time dependency on Clojure
            java.lang.reflect.Method getDataMethod = throwable.getClass().getMethod("getData");
            Object data = getDataMethod.invoke(throwable);
            if (data != null) {
                return data.toString();
            }
        } catch (Exception e) {
            // If reflection fails, return null
        }
        return null;
    }

    private void formatDefault(Throwable throwable, StringBuilder toAppendTo) {
        if (shortFormat) {
            // Short format: just the first line
            toAppendTo.append(throwable.getClass().getName());
            if (throwable.getMessage() != null) {
                toAppendTo.append(": ").append(throwable.getMessage());
            }
        } else {
            // Full stack trace
            toAppendTo.append(throwable.getClass().getName());
            if (throwable.getMessage() != null) {
                toAppendTo.append(": ").append(throwable.getMessage());
            }
            toAppendTo.append(System.lineSeparator());
            appendStackTrace(throwable, toAppendTo);
        }
    }

    private void appendStackTrace(Throwable throwable, StringBuilder toAppendTo) {
        StackTraceElement[] stackTrace = throwable.getStackTrace();
        int linesToPrint = Math.min(stackTrace.length, lines);

        for (int i = 0; i < linesToPrint; i++) {
            toAppendTo.append("\tat ").append(stackTrace[i]).append(System.lineSeparator());
        }

        if (linesToPrint < stackTrace.length) {
            toAppendTo.append("\t... ").append(stackTrace.length - linesToPrint).append(" more").append(System.lineSeparator());
        }

        // Handle cause chain
        Throwable cause = throwable.getCause();
        if (cause != null && lines > linesToPrint) {
            appendCause(cause, toAppendTo, stackTrace, lines - linesToPrint);
        }
    }

    private void appendCause(Throwable cause, StringBuilder toAppendTo,
                            StackTraceElement[] enclosingTrace, int remainingLines) {
        if (remainingLines <= 0) {
            return;
        }

        toAppendTo.append("Caused by: ");

        // Check if cause is also ExceptionInfo
        if (isExceptionInfo(cause)) {
            toAppendTo.append(cause.getClass().getName());
            if (cause.getMessage() != null) {
                toAppendTo.append(": ").append(cause.getMessage());
            }
            String data = getExceptionInfoData(cause);
            if (data != null) {
                toAppendTo.append(" ").append(data);
            }
        } else {
            toAppendTo.append(cause.getClass().getName());
            if (cause.getMessage() != null) {
                toAppendTo.append(": ").append(cause.getMessage());
            }
        }
        toAppendTo.append(System.lineSeparator());

        StackTraceElement[] causeTrace = cause.getStackTrace();

        // Find common frames with enclosing exception
        int m = causeTrace.length - 1;
        int n = enclosingTrace.length - 1;
        while (m >= 0 && n >= 0 && causeTrace[m].equals(enclosingTrace[n])) {
            m--;
            n--;
        }
        int commonFrames = causeTrace.length - 1 - m;
        int framesToPrint = Math.min(m + 1, remainingLines);

        for (int i = 0; i < framesToPrint; i++) {
            toAppendTo.append("\tat ").append(causeTrace[i]).append(System.lineSeparator());
        }

        if (commonFrames > 0) {
            toAppendTo.append("\t... ").append(commonFrames).append(" common frames omitted").append(System.lineSeparator());
        }

        // Recurse for nested causes
        Throwable nestedCause = cause.getCause();
        if (nestedCause != null) {
            appendCause(nestedCause, toAppendTo, causeTrace, remainingLines - framesToPrint);
        }
    }
}
