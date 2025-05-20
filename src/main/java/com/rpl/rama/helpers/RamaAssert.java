package com.rpl.rama.helpers;

import com.rpl.rama.Block;
import com.rpl.rama.Expr;
import com.rpl.rama.Helpers;
import com.rpl.rama.impl.NativeAnyArityRamaFunction;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.ops.RamaFunction1;
import com.rpl.rama.ops.RamaFunction2;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.RT;

public class RamaAssert {

  static final IFn assertVar;
  static final IFn deref;

  static {
    RT.init();
    assertVar = Clojure.var("clojure.core", "*assert*");
    deref = Clojure.var("clojure.core", "deref");
  }

  protected static boolean isAssertEnabled() {
    return (boolean)deref.invoke(assertVar);
  }

  private static <T0>  Object failedAssert1(T0 t0) {
    throw new AssertionError("Assertion failed: arg " + t0);
  }

  private static <T0, T1>  Object failedAssert2(T0 t0, T1 t1) {
    throw new AssertionError("Assertion failed, arg0: " + t0 + ", arg1: " + t1);
  }

  public static <T> Block assertMacro(RamaFunction1<T, Boolean> fn, Object arg) {
    final String assertResultVar = Helpers.genVar("assertResult");
    if (isAssertEnabled()) {
      return
          Block
          .each(fn, arg).out(assertResultVar)
          .ifTrue(
            new Expr(Ops.NOT, new Expr(Ops.IDENTITY, assertResultVar)),
            Block.each(RamaAssert::failedAssert1, arg));
    } else {
      // TODO is there a better NoOp?
      // return Block.each(Ops.IDENTITY, 1).out("*noop");
      // TODO try null as well
      return Block.create();
    }
  }

  public static <T> Block assertMacro(NativeAnyArityRamaFunction fn, Object arg) {
    final String assertResultVar = Helpers.genVar("assertResult");
    if (isAssertEnabled()) {
      return
	Block
	.each(fn, arg).out(assertResultVar)
	.ifTrue(
		new Expr(Ops.NOT, new Expr(Ops.IDENTITY, assertResultVar)),
		Block.each(RamaAssert::failedAssert1, arg));
    } else {
      return Block.create();
    }
  }

  public static <T> Block assertMacro(NativeAnyArityRamaFunction fn, Object arg0, Object arg1) {
    final String assertResultVar = Helpers.genVar("assertResult");
    if (isAssertEnabled()) {
      return
	Block
	.each(fn, arg0, arg1).out(assertResultVar)
	.ifTrue(
		new Expr(Ops.NOT, new Expr(Ops.IDENTITY, assertResultVar)),
		Block.each(RamaAssert::failedAssert2, arg0, arg1));
    } else {
      return Block.create();
    }
  }

  // TODO Add method for AnyArity with Object... vararg

  public static <T, U> Block assertMacro(RamaFunction2<T, U, Boolean> fn, Object arg0, Object arg1) {
    final String assertResultVar = Helpers.genVar("assertResult");
    if (isAssertEnabled()) {
      return
          Block
          .each(fn, arg0, arg1).out(assertResultVar)
          .ifTrue(
            new Expr(Ops.NOT, new Expr(Ops.IDENTITY, assertResultVar)),
            Block.each(RamaAssert::failedAssert2, arg0, arg1));
    } else {
      // TODO is there a better NoOp?
      return Block.each(Ops.IDENTITY, 1).out("*noop");
    }
  }

};
