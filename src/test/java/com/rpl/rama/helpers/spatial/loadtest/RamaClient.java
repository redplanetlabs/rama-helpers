package com.rpl.rama.helpers.spatial.loadtest;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

import clojure.java.api.Clojure;
import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;

public class RamaClient {

  private static Var _require;
  private static Var _set_topology_active;

  static {
    RT.init();
    _require = (Var) Clojure.var("clojure.core", "require");

    _require.invoke(
      Symbol.intern("rpl.rama.distributed.command.set-topology-active"));

    _set_topology_active =
        (Var) Clojure.var("rpl.rama.distributed.command.set-topology-active",
                          "-main");
  }

  public static Boolean setTopologyActive(final String moduleName,
                                          final String topologyName,
                                          final Boolean state) {
    _set_topology_active.invoke(moduleName, topologyName, state ? "true" : "false", "--useInternalHostnames");
    return state;
  }

  public static Boolean createRamaYaml() throws Exception {
    File ramaYaml = new File("target/test-classes/rama.yaml");
    try (PrintWriter pw = new PrintWriter(new FileWriter(ramaYaml))) {
      pw.println("conductor.host: localhost");
    }
    return true;
  }
}
