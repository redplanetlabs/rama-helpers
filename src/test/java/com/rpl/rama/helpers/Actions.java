package com.rpl.rama.helpers;

import com.rpl.rama.RamaSerializable;

public class Actions {

  public static class AddItem implements RamaSerializable {
    public Object key;
    public Object item;
    public AddItem(Object key, Object item) { this.key = key; this.item = item; }
  }
  public static class RemoveItem implements RamaSerializable {
    public Object key;
    public Object item;
    public RemoveItem(Object key, Object item) { this.key = key; this.item = item; }
  }
  public static class RemoveItemById implements RamaSerializable {
    public Object key;
    public Object id;
    public RemoveItemById(Object key, Object id) { this.key = key; this.id = id; }
  }
  public static class RemoveItemByEntityId implements RamaSerializable {
    public Object key;
    public Object id;
    public RemoveItemByEntityId(Object key, Object id) { this.key = key; this.id = id; }
  }
  public static class RemoveKey implements RamaSerializable {
    public Object key;
    public RemoveKey(Object key) { this.key = key; }
  }
  public static class ClearItems implements RamaSerializable {
    public Object key;
    public ClearItems(Object key) { this.key = key; }
  }
}
