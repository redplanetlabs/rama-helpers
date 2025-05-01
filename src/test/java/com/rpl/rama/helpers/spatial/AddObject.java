package com.rpl.rama.helpers.spatial;

import com.rpl.rama.RamaSerializable;

/** Depot value */
public class AddObject implements RamaSerializable {
  public final MBR bounds;
  public final Object object;

  public AddObject(final MBR bounds, Object object) {
    this.bounds = bounds;
    this.object = object;
  }

  @Override
  public String toString() {
    return "AddObject [bounds=" + bounds + ", object=" + object + "]";
  }
}
