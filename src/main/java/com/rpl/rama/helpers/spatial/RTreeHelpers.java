package com.rpl.rama.helpers.spatial;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

class RTreeHelpers {
  public static <T> List<T> newArrayList() {
    return new ArrayList<>();
  }

  public static void dumpBoundsList(List<List<Object>> boundsList) throws IOException {
    ArrayList<PrintWriter> levelWriters = new ArrayList<>();
    for (List<Object> tuple : boundsList) {
      int level = (Integer) tuple.get(0);
      String s = (String) tuple.get(1);
      if (level >= levelWriters.size()) {
        FileWriter fw = new FileWriter("level-" + level + "-bounds.txt");
        levelWriters.add(level, new PrintWriter(fw));
      }
      PrintWriter writer = levelWriters.get(level);
      writer.println(s);
    }

    for (PrintWriter pw : levelWriters) {
      pw.close();
    }
  }
}
