package com.rpl.rama.helpers;

import com.rpl.rama.Block;
import com.rpl.rama.Expr;
import com.rpl.rama.Helpers;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.ops.RamaFunction1;

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

  private static <T0>  Object failedAssert(T0 t0) {
    throw new AssertionError("Assertion failed: arg " + t0);
  }

  public static <T> Block assertMacro(RamaFunction1<T, Boolean> fn, Object arg) {
    final String assertResultVar = Helpers.genVar("assertResult");
    if (isAssertEnabled()) {
      return
          Block
          .each(fn, arg).out(assertResultVar)
          .ifTrue(
            new Expr(Ops.NOT, new Expr(Ops.IDENTITY, assertResultVar)),
            Block.each(RamaAssert::failedAssert, arg));
    } else {
      // TODO is there a better NoOp?
      return Block.each(Ops.IDENTITY, 1).out("*noop");
    }
  }

};
