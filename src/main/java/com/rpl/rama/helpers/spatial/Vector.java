package com.rpl.rama.helpers.spatial;

import java.util.List;

import clojure.java.api.Clojure;
import clojure.lang.PersistentVector;
import clojure.lang.RT;
import clojure.lang.Var;

/** Helpers for working with clojure PersistentVector */
class Vector {

  public static PersistentVector empty() {
    return PersistentVector.EMPTY;
  }

  public static PersistentVector conj(PersistentVector l1, Object obj) {
    return l1.cons(obj);
  }

  private static Var _into;
  private static Var _partition;
  private static Var _partitionAll;

  static {
    RT.init();
    _into = (Var) Clojure.var("clojure.core", "into");
    _partition = (Var) Clojure.var("clojure.core", "partition");
    _partitionAll = (Var) Clojure.var("clojure.core", "partition-all");
    assert _into != null;
  }

  public static PersistentVector into(PersistentVector l1, List l2) {
    return (PersistentVector)_into.invoke(l1, l2);
  }

  public static PersistentVector partition(long n, PersistentVector l1) {
    return Vector.into(Vector.empty(), (List)_partition.invoke(n, l1));
  }

  public static PersistentVector partitionAll(long n, PersistentVector l1) {
    return Vector.into(Vector.empty(), (List)_partitionAll.invoke(n, l1));
  }

  public static Object peek(PersistentVector v) {
    return RT.peek(v);
  }

  public static Object pop(PersistentVector v) {
    return RT.pop(v);
  }
}
