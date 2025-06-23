package com.rpl.rama.helpers.spatial.loadtest;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import com.rpl.rama.RamaSerializable;
import com.rpl.rama.helpers.spatial.AddObject;
import com.rpl.rama.helpers.spatial.MBR;

public class RandomObjectGenerator implements Loader, RamaSerializable {
  final MBR bounds;
  final int batchSize;
  int total;

  public RandomObjectGenerator(MBR bounds, int batchSize) {
    this.bounds = bounds;
    this.batchSize = batchSize;
    this.total = 0;
  }

  public int getTotal() {
    return total;
  }

  public static class RandomObject {
    final public MBR bounds;
    final public long id;

    public RandomObject(MBR bounds, long id) {
      this.bounds = bounds;
      this.id = id;
    }

    @Override
    public String toString() {
      return "RandomObject [bounds=" + bounds + ", id=" + id + "]";
    }
  }

  public List<RandomObject> generateObjects(Random random,
                                            final int numObjects) {
    List<RandomObject> objects = new ArrayList<RandomObject>();
    for (long i = 0; i < numObjects; i++) {
      boolean isIntersect = random.nextBoolean();
      if (isIntersect && !objects.isEmpty()) {
        int elementIndex = random.nextInt(objects.size());
        RandomObject element = objects.get(elementIndex);
        MBR objectBounds = element.bounds.randomSubBounds(random);
        objects.add(new RandomObject(objectBounds, i));
      } else {
        MBR objectBounds = bounds.randomSubBounds(random);
        objects.add(new RandomObject(objectBounds, i));
      }
    }
    total = total + numObjects;
    return objects;
  }

  public LoadDataResult loadData(Random random) {
    return new LoadDataResult(
      false,
      generateObjects(random, batchSize)
      .stream()
      .map(
        (RandomObject ro) -> { return new AddObject(ro.bounds, ro.id); })
      .collect(Collectors.toList()));
  }
}
