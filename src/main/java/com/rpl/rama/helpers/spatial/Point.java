package com.rpl.rama.helpers.spatial;

import com.rpl.rama.RamaSerializable;

class Point implements RamaSerializable {
  private final double[] coords;
  private final int dimensions;

  public Point(double[] coords) {
    this.coords = coords;
    this.dimensions = coords.length;
  }
}
