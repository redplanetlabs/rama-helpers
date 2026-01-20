package com.rpl.rama.helpers.log4j2;

import clojure.lang.ExceptionInfo;
import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.message.SimpleMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ExceptionInfoConverter")
public class ExceptionInfoConverterTest {

  private static final String LINE_SEP = System.lineSeparator();

  private LogEvent createLogEvent(Throwable thrown) {
    return Log4jLogEvent.newBuilder()
      .setLevel(Level.ERROR)
      .setMessage(new SimpleMessage("test"))
      .setThrown(thrown)
      .build();
  }

  private ExceptionInfo createExceptionInfo(String msg, Object... kvs) {
    IPersistentMap data = PersistentHashMap.EMPTY;
    for (int i = 0; i < kvs.length; i += 2) {
      data = data.assoc(kvs[i], kvs[i + 1]);
    }
    return new ExceptionInfo(msg, data);
  }

  private ExceptionInfo createExceptionInfo(String msg, Throwable cause, Object... kvs) {
    IPersistentMap data = PersistentHashMap.EMPTY;
    for (int i = 0; i < kvs.length; i += 2) {
      data = data.assoc(kvs[i], kvs[i + 1]);
    }
    return new ExceptionInfo(msg, data, cause);
  }

  @Nested
  @DisplayName("with no options (full format)")
  class FullFormat {

    @Test
    @DisplayName("formats ExceptionInfo with data")
    void formatsExceptionInfoWithData() {
      ExceptionInfoConverter converter = ExceptionInfoConverter.newInstance(null);
      ExceptionInfo ex = createExceptionInfo("oops", Keyword.intern("code"), 42);
      StringBuilder sb = new StringBuilder();

      converter.format(createLogEvent(ex), sb);
      String result = sb.toString();

      assertTrue(result.startsWith("clojure.lang.ExceptionInfo: oops {:code 42}"),
        "output: " + result);
      assertTrue(result.contains("\tat "), "output: " + result);
    }

    @Test
    @DisplayName("formats regular exception without data")
    void formatsRegularException() {
      ExceptionInfoConverter converter = ExceptionInfoConverter.newInstance(null);
      RuntimeException ex = new RuntimeException("boom");
      StringBuilder sb = new StringBuilder();

      converter.format(createLogEvent(ex), sb);
      String result = sb.toString();

      assertTrue(result.startsWith("java.lang.RuntimeException: boom"),
        "output: " + result);
      assertTrue(result.contains("\tat "), "output: " + result);
    }

    @Test
    @DisplayName("outputs nothing when no exception")
    void outputsNothingWhenNoException() {
      ExceptionInfoConverter converter = ExceptionInfoConverter.newInstance(null);
      LogEvent event = Log4jLogEvent.newBuilder()
        .setLevel(Level.ERROR)
        .setMessage(new SimpleMessage("test"))
        .build();
      StringBuilder sb = new StringBuilder();

      converter.format(event, sb);

      assertEquals("", sb.toString());
    }
  }

  @Nested
  @DisplayName("with short option")
  class ShortFormat {

    @Test
    @DisplayName("formats ExceptionInfo on single line with data")
    void formatsExceptionInfoOnSingleLine() {
      ExceptionInfoConverter converter = ExceptionInfoConverter.newInstance(new String[]{"short"});
      ExceptionInfo ex = createExceptionInfo("oops", Keyword.intern("x"), 1);
      StringBuilder sb = new StringBuilder();

      converter.format(createLogEvent(ex), sb);
      String result = sb.toString();

      assertEquals("clojure.lang.ExceptionInfo: oops {:x 1}", result);
    }

    @Test
    @DisplayName("formats regular exception on single line")
    void formatsRegularExceptionOnSingleLine() {
      ExceptionInfoConverter converter = ExceptionInfoConverter.newInstance(new String[]{"short"});
      RuntimeException ex = new RuntimeException("boom");
      StringBuilder sb = new StringBuilder();

      converter.format(createLogEvent(ex), sb);
      String result = sb.toString();

      assertEquals("java.lang.RuntimeException: boom", result);
    }
  }

  @Nested
  @DisplayName("with none option")
  class NoneFormat {

    @Test
    @DisplayName("outputs nothing")
    void outputsNothing() {
      ExceptionInfoConverter converter = ExceptionInfoConverter.newInstance(new String[]{"none"});
      ExceptionInfo ex = createExceptionInfo("oops", Keyword.intern("x"), 1);
      StringBuilder sb = new StringBuilder();

      converter.format(createLogEvent(ex), sb);

      assertEquals("", sb.toString());
    }

    @Test
    @DisplayName("outputs nothing with 0 option")
    void outputsNothingWithZero() {
      ExceptionInfoConverter converter = ExceptionInfoConverter.newInstance(new String[]{"0"});
      ExceptionInfo ex = createExceptionInfo("oops", Keyword.intern("x"), 1);
      StringBuilder sb = new StringBuilder();

      converter.format(createLogEvent(ex), sb);

      assertEquals("", sb.toString());
    }
  }

  @Nested
  @DisplayName("with line limit option")
  class LineLimitFormat {

    @Test
    @DisplayName("limits stack trace lines")
    void limitsStackTraceLines() {
      ExceptionInfoConverter converter = ExceptionInfoConverter.newInstance(new String[]{"2"});
      ExceptionInfo ex = createExceptionInfo("oops", Keyword.intern("x"), 1);
      StringBuilder sb = new StringBuilder();

      converter.format(createLogEvent(ex), sb);
      String result = sb.toString();

      String[] lines = result.split(LINE_SEP);
      // First line is the exception, then 2 stack frames, then "... N more"
      assertTrue(lines.length >= 3, "lines: " + lines.length);
      assertTrue(lines[0].contains("ExceptionInfo: oops {:x 1}"),
        "first line: " + lines[0]);
      assertTrue(lines[1].startsWith("\tat "), "line 1: " + lines[1]);
      assertTrue(lines[2].startsWith("\tat "), "line 2: " + lines[2]);
    }
  }

  @Nested
  @DisplayName("with nested causes")
  class NestedCauses {

    @Test
    @DisplayName("includes cause chain with data")
    void includesCauseChainWithData() {
      ExceptionInfoConverter converter = ExceptionInfoConverter.newInstance(new String[]{"short"});
      ExceptionInfo inner = createExceptionInfo("inner", Keyword.intern("a"), 1);
      ExceptionInfo outer = createExceptionInfo("outer", inner, Keyword.intern("b"), 2);
      StringBuilder sb = new StringBuilder();

      converter.format(createLogEvent(outer), sb);
      String result = sb.toString();

      // Short format only shows outer exception
      assertEquals("clojure.lang.ExceptionInfo: outer {:b 2}", result);
    }

    @Test
    @DisplayName("shows nested ExceptionInfo data in full format")
    void showsNestedDataInFullFormat() {
      ExceptionInfoConverter converter = ExceptionInfoConverter.newInstance(null);
      ExceptionInfo inner = createExceptionInfo("inner", Keyword.intern("a"), 1);
      ExceptionInfo outer = createExceptionInfo("outer", inner, Keyword.intern("b"), 2);
      StringBuilder sb = new StringBuilder();

      converter.format(createLogEvent(outer), sb);
      String result = sb.toString();

      assertTrue(result.contains("outer {:b 2}"), "output: " + result);
      assertTrue(result.contains("Caused by: clojure.lang.ExceptionInfo: inner {:a 1}"),
        "output: " + result);
    }

    @Test
    @DisplayName("shows regular cause without data")
    void showsRegularCauseWithoutData() {
      ExceptionInfoConverter converter = ExceptionInfoConverter.newInstance(null);
      RuntimeException inner = new RuntimeException("root cause");
      ExceptionInfo outer = createExceptionInfo("wrapper", inner, Keyword.intern("x"), 1);
      StringBuilder sb = new StringBuilder();

      converter.format(createLogEvent(outer), sb);
      String result = sb.toString();

      assertTrue(result.contains("wrapper {:x 1}"), "output: " + result);
      assertTrue(result.contains("Caused by: java.lang.RuntimeException: root cause"),
        "output: " + result);
    }
  }

  @Nested
  @DisplayName("newInstance factory")
  class Factory {

    @Test
    @DisplayName("creates converter with null options")
    void createsWithNullOptions() {
      ExceptionInfoConverter converter = ExceptionInfoConverter.newInstance(null);
      assertNotNull(converter);
    }

    @Test
    @DisplayName("creates converter with empty options")
    void createsWithEmptyOptions() {
      ExceptionInfoConverter converter = ExceptionInfoConverter.newInstance(new String[]{});
      assertNotNull(converter);
    }

    @Test
    @DisplayName("creates converter with valid options")
    void createsWithValidOptions() {
      ExceptionInfoConverter converter = ExceptionInfoConverter.newInstance(new String[]{"short"});
      assertNotNull(converter);
    }
  }
}
