package com.rpl.rama.helpers.spatial;

import com.rpl.rama.RamaSerializable;
import com.rpl.rama.ops.OutputCollector;

public class ModificationCollector {

  public static class AddObject implements RamaSerializable {
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

  public ModificationCollector(OutputCollector collector) {
    this.collector = collector;
  }

  public void addObject(final MBR bounds, final Long objectId) {
    collector.emit(new AddObject(bounds, objectId));
  }

}
