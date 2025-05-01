package com.rpl.rama.helpers;

import static org.mockito.Mockito.*;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.rpl.rama.Block;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import clojure.lang.RT;
import clojure.lang.Var;

public class RamaAssertTest {

    @Test
    public void isAssertEnabledTest() throws Exception {
      RT.init();
      Var assertVar = RT.var("clojure.core", "*assert*");

      try {
	Var.pushThreadBindings(RT.map(assertVar, false));
	assertVar.set(false);
	assertFalse(RamaAssert.isAssertEnabled());
      } finally {
	 Var.popThreadBindings();
      }
      try {
	Var.pushThreadBindings(RT.map(assertVar, true));
	assertVar.set(true);
	assertTrue(RamaAssert.isAssertEnabled());
      } finally {
	 Var.popThreadBindings();
      }
    }

    @Test
    public void assertEnabledTest() throws Exception {

      try (MockedStatic<RamaAssert> mockedAssert
	   = mockStatic(RamaAssert.class, Mockito.CALLS_REAL_METHODS)) {

	mockedAssert.when(() -> RamaAssert.isAssertEnabled()).thenReturn(true);

	assertTrue(RamaAssert.isAssertEnabled());

	Block.macro(RamaAssert.assertMacro((Boolean arg) -> {
	      return arg;
	    }, true))
	  .execute();

	Error e = assertThrows(AssertionError.class,
			       () -> {
				 Block
				   .macro(RamaAssert.assertMacro((Boolean arg) -> {
					 return arg;
				       }, false))
				   .execute();
			       });

	assertEquals("Assertion failed: arg false", e.getMessage());
      }
    }

    @Test
    public void assertDisabledTest() throws Exception {

      try (MockedStatic<RamaAssert> mockedAssert
	   = mockStatic(RamaAssert.class, Mockito.CALLS_REAL_METHODS)) {

	mockedAssert.when(() -> RamaAssert.isAssertEnabled()).thenReturn(false);

        assertFalse(RamaAssert.isAssertEnabled());

	AtomicBoolean isCalled = new AtomicBoolean(false);

        Block.macro(RamaAssert.assertMacro((Boolean arg) -> {
	      isCalled.set(true);
	      return arg;
	    }, true))
	  .execute();

	assertFalse(isCalled.get());

	isCalled.set(false);

	Block.macro(RamaAssert.assertMacro((Boolean arg) -> {
	      isCalled.set(true);
	      return arg;
	    }, false))
	  .execute();

	assertFalse(isCalled.get());
      }
    }
}
