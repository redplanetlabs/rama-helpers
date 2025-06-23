package com.rpl.rama.helpers.spatial.loadtest;

import java.util.Random;

public interface Loader {
  LoadDataResult loadData(Random random);

  int getTotal();
}
