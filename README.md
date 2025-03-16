# rama-helpers

This project contains helpful utilities for developing [Rama](https://redplanetlabs.com/docs/~/index.html) modules.

- `ModuleUniqueIdPState`: Generates 64 bit IDs guaranteed to be unique across the whole module.
- `TaskUniqueIdPState`: Generates 32 or 64 bit IDs unique on the task.
- `TopologyScheduler`: Schedules future work for a topology in a robust and fault-tolerant way.
- `KeyToLinkedEntitySetPStateGroup`: Implements map of linked sets data structure. Inner sets can be efficiently queried by membership or by order of insertion.
- `KeyToFixedItemsPStateGroup`: Implements map of fixed lists data structure. Lists of values automatically drop their oldest elements on write when exceeding the configured max size.
- `KeyToUniqueFixedItemsPStateGroup`: Like `KeyToFixedItemsPStateGroup` but also ensures values in inner lists are unique.
- `TaskGlobalField`: Declare an object at the module level to be locally available on every task.
- `TopologyUtils`: Assorted Java functions useful for development and testing.

## Maven

`rama-helpers` is available via a Maven repository managed by Red Planet Labs. Here is the repository information you can add to your pom.xml file:

```
<repositories>
  <repository>
    <id>nexus-releases</id>
    <url>https://nexus.redplanetlabs.com/repository/maven-public-releases</url>
  </repository>
</repositories>
```

Here is the dependency declaration:

```
<dependency>
    <groupId>com.rpl</groupId>
    <artifactId>rama-helpers</artifactId>
    <version>0.10.0</version>
</dependency>
```
