package com.rpl.rama.helpers.spatial.loadtest;

import java.util.List;

import com.rpl.rama.helpers.spatial.AddObject;

public class LoadDataResult {
  public Boolean done;
  public List<AddObject> addObjects;

  public LoadDataResult(Boolean done, List<AddObject> addObjects) {
    this.addObjects = addObjects;
    this.done = done;
  }

  @Override
  public String toString() {
    return "LoadDataResult [done=" + done +
        ", addObjects=" + addObjects + "]";
  }
}
