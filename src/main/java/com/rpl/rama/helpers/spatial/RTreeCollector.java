package com.rpl.rama.helpers.spatial;

import com.rpl.rama.ops.OutputCollector;

public class RTreeCollector {

  public static class AddObject {
    public final MBR bounds;
    public final Long objectId;

    public AddObject(MBR bounds, Long objectId) {
      this.bounds = bounds;
      this.objectId = objectId;
    }

    public static AddObject mkAddObject(MBR bounds, Long objectId) {
      return new AddObject(bounds, objectId);
    }

    @Override
    public String toString() {
      return "AddObject [bounds=" + bounds + ", objectId=" + objectId + "]";
    }
  }

  private final OutputCollector collector;

  public RTreeCollector(OutputCollector collector) {
    this.collector = collector;
  }

  public void addObject(final MBR bounds, final Long objectId) {
    collector.emit(new AddObject(bounds, objectId));
  }

}
