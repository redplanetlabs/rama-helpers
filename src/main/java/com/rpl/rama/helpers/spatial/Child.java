package com.rpl.rama.helpers.spatial;

import com.rpl.rama.RamaSerializable;

public final class Child implements RamaSerializable {
  public final MBR bounds;
  public final long id;

  public Child(MBR bounds, long id) {
    this.bounds = bounds;
    this.id = id;
  }

  @Override
  public String toString() {
    return "Child [bounds=" + bounds + ", id=" + id + "]";
  }

  long childId() {
    return id;
  }

  public static boolean isChild(Object x) {
    return x instanceof Child;
  }
}
