package com.rpl.rama.helpers.spatial;

import com.rpl.rama.Block;
import com.rpl.rama.PState;
import com.rpl.rama.Path;
import com.rpl.rama.helpers.ModuleUniqueIdPState;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.ops.RamaFunction1;
import com.rpl.rama.ops.RamaFunction2;
import com.rpl.rama.test.TestPState;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.junit.Test;

public class RTreeUnitTest {

  @Test
  public void updateNodeTest() throws Exception {
    final int branchingFactor = 2;
    TestModuleUniqueIdPState idGenerator = new TestModuleUniqueIdPState();
    Node rootNode = new LeafNode(-1, -1);
    List<RTreeCollector.AddObject> ops = new ArrayList<>();
    try (TestPState objectId = TestPState.create(Long.class)) {
      Block
          .each(Ops.IDENTITY, objectId).out("$$objectId")
          .each(Ops.IDENTITY, rootNode).out("*node")
          .each(Ops.IDENTITY, ops).out("*nodeOps")
          .macro(RTree.updateNode(
            branchingFactor,
            idGenerator,
            "*node",
            "*nodeOps",
            "*newSiblings"))
          .execute();
      assertNull(objectId.selectOne(Path.stay()));
      assertEquals(0, rootNode.count());
    }

    final double[] origin = {0, 0};
    final double[] ones = {1, 1};
    final double[] twos = {2, 2};
    final double[] oneHundreds = {100, 100};
    final double[] twoHundreds = {200, 200};

    final MBR oneBounds = new MBR(origin, ones);
    final MBR twoBounds = new MBR(origin, twos);
    final MBR twoHundredBounds = new MBR(oneHundreds, twoHundreds);

    VarRef<Node> node = new VarRef<>("*node");
    VarRef<List<Node>> newSiblings = new VarRef<>("*newSiblings");

    // Create a root node with one child
    ops.add(new RTreeCollector.AddObject(oneBounds, new Long(1)));
    {
      Block
          .each(Ops.IDENTITY, rootNode).out(node.name)
          .each(Ops.IDENTITY, ops).out("*nodeOps")
          .macro(RTree.updateNode(
            branchingFactor,
            idGenerator,
            node.name,
            "*nodeOps",
            newSiblings.name))
          .macro(node.capture())
          .execute();
      assertEquals("node has one child", 1, node.get().count());
      assertEquals(oneBounds, node.get().children.get(0).bounds);
      assertEquals(1, node.get().children.get(0).id);
    }

    // Create a full root node
    ops.add(new RTreeCollector.AddObject(oneBounds, new Long(1)));
    ops.add(new RTreeCollector.AddObject(twoBounds, new Long(2)));
    assertEquals(2, ops.size());
    {
      Block
          .each(Ops.IDENTITY, rootNode).out(node.name)
          .each(Ops.IDENTITY, ops).out("*nodeOps")
          .macro(RTree.updateNode(
            branchingFactor,
            idGenerator,
            node.name,
            "*nodeOps",
            newSiblings.name))
          .macro(node.capture())
          .execute();
      assertEquals("node has two child", 2, node.get().count());
      assertEquals(oneBounds, node.get().children.get(0).bounds);
      assertEquals(twoBounds, node.get().children.get(1).bounds);
      assertEquals(1, node.get().children.get(0).id);
      assertEquals(2, node.get().children.get(1).id);
    }

    // Create an over full root node, requiring a split
    ops.add(new RTreeCollector.AddObject(oneBounds, new Long(1)));
    ops.add(new RTreeCollector.AddObject(twoBounds, new Long(2)));
    ops.add(new RTreeCollector.AddObject(twoHundredBounds, new Long(200)));
    assertEquals(3, ops.size());
    {
      Block
          .each(Ops.IDENTITY, rootNode).out(node.name)
          .each(Ops.IDENTITY, ops).out("*nodeOps")
          .macro(RTree.updateNode(
            branchingFactor,
            idGenerator,
            node.name,
            "*nodeOps",
            newSiblings.name))
          .macro(node.capture())
          .macro(newSiblings.capture())
          .execute();
      assertEquals("a new sibling created", 1, newSiblings.get().size());
      assertEquals("node has two child", 2, node.get().count());
      assertEquals(oneBounds, node.get().children.get(0).bounds);
      assertEquals(twoBounds, node.get().children.get(1).bounds);
      assertEquals(1, node.get().children.get(0).id);
      assertEquals(2, node.get().children.get(1).id);
    }
  }

  @Test
  public void varRefTest() throws Exception {
    VarRef<String> fred = new VarRef<>("*fred");

    Block.each(Ops.IDENTITY, "fred").out(fred.name)
        .macro(fred.capture())
        .execute();
      assertEquals("fred", fred.get());
    }
}
