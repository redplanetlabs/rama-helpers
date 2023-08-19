package com.rpl.rama.helpers;

import com.rpl.rama.ops.*;

public class TestHelpers {
  public static void attainCondition(RamaFunction0<Boolean> fn) {
    long start = System.nanoTime();
    while(true) {
      if(fn.invoke()) {
        break;
      } else if(System.nanoTime() - start >= 45000000000L) { // 45 seconds
        throw new RuntimeException("Failed to attain condition");
      } else {
        try {
          Thread.sleep(2);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  public static void attainStableCondition(RamaFunction0<Boolean> fn) {
    long start = System.nanoTime();
    int reachedCount = 0;
    try {
      while(true) {
        if(fn.invoke()) {
          reachedCount++;
          if(reachedCount > 25) break;
          Thread.sleep(2);
        } else if(reachedCount > 0) {
          throw new RuntimeException("Condition not stable");
        } else if(System.nanoTime() - start >= 45000000000L) { // 45 seconds
          throw new RuntimeException("Failed to attain condition");
        } else {
          Thread.sleep(2);
        }
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}