package com.rpl.rama.helpers.spatial;

import java.util.concurrent.atomic.AtomicReference;

import com.rpl.rama.Block;

public class VarRef<T extends Object> {
  private final AtomicReference<T> ref;
  public final String name;

  public VarRef(String name) {
    this.ref = new AtomicReference<>();
    this.name = name;
  }

  public Block capture() {
    return Block.each(
      (AtomicReference<T> ref, T value) -> {
        ref.set(value);
        return value;
      },
      ref,
      name);
  }

  public T get() {
    return ref.get();
  }
}
