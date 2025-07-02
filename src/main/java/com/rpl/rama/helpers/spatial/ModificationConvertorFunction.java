package com.rpl.rama.helpers.spatial;

import com.rpl.rama.RamaSerializable;

/** Functional interface for expected dataConverter signature */
public interface ModificationConvertorFunction<T>  extends RamaSerializable {
  public void invoke(T data, ModificationCollector collector);
}
