package com.rpl.rama.helpers;

import java.util.*;

import com.rpl.rama.*;
import com.rpl.rama.integration.*;

/**
 * Task global implementation containing a single mutable field.
 *
 * @see <a href="https://beta.redplanetlabs.com/docs/docs/1.0.0/integrating.html">Integration documentation</a>
 */
public class TaskGlobalField implements TaskGlobalObject {
  public Object field;

  public TaskGlobalField(Object init) {
    this.field = init;
  }

  public void prepareForTask(int taskId, TaskGlobalContext context) { }

  public void close() { }
}
