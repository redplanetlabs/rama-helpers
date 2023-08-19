package com.rpl.rama.helpers;

import com.rpl.rama.*;
import org.junit.Test;
import static org.junit.jupiter.api.Assertions.*;

public class TopologyUtilsTest {
  public static class Foo implements RamaSerializable {
    public String a;
    public Integer b;
  }


  @Test
  public void getFieldByNameTest() {
    Foo f = new Foo();
    f.a = "sss";
    f.b = 10;

    String s = TopologyUtils.getFieldByName(f, "a");
    assertEquals("sss", s);
    Integer i = TopologyUtils.getFieldByName(f, "b");
    assertEquals(10, i);
    assertThrows(Exception.class, () -> TopologyUtils.getFieldByName(f, "c"));
  }

  private static Foo testFoo;

  @Test
  public void extractJavaFieldsTest() {
    testFoo = new Foo();

    Block.each(() -> {
            Foo f = new Foo();
            f.a = "aaa";
            f.b = 2;
            return f;
          }).out("*foo")
         .macro(TopologyUtils.extractJavaFields("*foo", "*a", "*b"))
         .each((String a, Integer b) -> {
            testFoo.a = a;
            testFoo.b = b;
            return null;
         }, "*a", "*b")
         .execute();

    assertEquals("aaa", testFoo.a);
    assertEquals(2, testFoo.b);
  }
}
