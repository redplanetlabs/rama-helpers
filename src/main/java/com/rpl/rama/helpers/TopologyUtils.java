package com.rpl.rama.helpers;

import com.rpl.rama.*;
import com.rpl.rama.ops.*;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Helper utilities for use within topology code
 *
 * @see <a href="https://redplanetlabs.com/docs/~/tutorial4.html">Dataflow documentation</a>
 * @see <a href="https://redplanetlabs.com/docs/~/intermediate-dataflow.html#_macros">Detailed macro documentation</a>
 */
public class TopologyUtils {

  public static final ConcurrentHashMap<Class, Map<String, Field>> FIELD_CACHE = new ConcurrentHashMap<>();

  public static Map<String, Field> getFieldCache(Class klass) {
    Map<String, Field> ret = FIELD_CACHE.get(klass);
    if(ret==null) {
      try {
        ret = new HashMap<>();
        for(Field f: klass.getFields()) {
          ret.put(f.getName(), f);
        }
        FIELD_CACHE.put(klass, ret);
      } catch(Exception e) {
        throw new RuntimeException(e);
      }
    }
    return ret;
  }

  public static <T> T getFieldByName(Object obj, String fieldName) {
    Field field = getFieldCache(obj.getClass()).get(fieldName);
    if(field==null) throw new RuntimeException("Field " + fieldName + " does not exist on " + obj.getClass());
    try {
      return (T) field.get(obj);
    } catch(IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }


  public static class ExtractJavaField implements RamaFunction1<Object, Object> {
    private String field;

    public ExtractJavaField(String f) {
      this.field = f;
    }

    @Override
    public Object invoke(Object obj) {
      return getFieldByName(obj, field);
    }
  }


  /**
   * Macro to extract public Java fields from a value
   *
   * @param from Runtime value from which to extract fields
   * @param fieldVars Fields to extract and vars to bind them to. For example, *field1 will extract field1 from the object and
   * bind it to the var *field1.
   * @see <a href="https://redplanetlabs.com/docs/~/intermediate-dataflow.html#_macros">Detailed macro documentation</a>
   */
  public static Block extractJavaFields(Object from, String... fieldVars) {
    Block.Impl ret = Block.create();
    for(String f: fieldVars) {
      String name;
      if(Helpers.isGeneratedVar(f)) name = Helpers.getGeneratedVarPrefix(f);
      else name = f.substring(1);
      ret = ret.each(new ExtractJavaField(name), from).out(f);
    }
    return ret;
  }


  private static volatile Long _simTime = null;

  /**
   * Causes {@link TopologyUtils#currentTimeMillis()} in this class to return a manually controlled value. Sim time starts at 0
   * and can be advanced with {@link advanceSimTime}.
   * <br><br>
   * This works by using global state, so tests using this facility must call {@link Closeable#close()} on the
   * returned object before exiting.
   */
  public static Closeable startSimTime() {
    _simTime = 0L;
    return new Closeable() {
      @Override
      public void close() throws IOException {
        _simTime = null;
      }
    };
  }

  /**
   * Advances sim time by the specified number of millis.
   */
  public static void advanceSimTime(long millis) {
    _simTime = _simTime + millis;
  }


  /**
   * If sim time is active, returns the sim time. Otherwise, returns {@link System#currentTimeMillis()}.
   */
  public static long currentTimeMillis() {
    if(_simTime!=null) return _simTime;
    else return System.currentTimeMillis();
  }
}
