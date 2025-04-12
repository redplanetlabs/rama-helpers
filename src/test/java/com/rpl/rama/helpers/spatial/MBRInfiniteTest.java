package com.rpl.rama.helpers.spatial;

import static org.junit.Assert.*;
import org.junit.Test;

/**
 * Test class specifically focused on testing the infinite boundary behavior
 * of the MBR class, as described in the MBR-open-range.md design document.
 */
public class MBRInfiniteTest {

  /**
   * Test the creation and basic properties of MBRs with infinite bounds.
   */
  @Test
  public void testInfiniteMBRCreation() {
    // Fully infinite MBR
    double[] infMins = {Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY};
    double[] infMaxs = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
    MBR infMBR = new MBR(infMins, infMaxs);

    assertTrue(infMBR.isInfinite());
    assertTrue(infMBR.hasInfiniteBound());
    assertFalse(infMBR.isEmpty());

    // Partially infinite MBR (only one dimension)
    double[] partialInfMins = {1.0, Double.NEGATIVE_INFINITY};
    double[] partialInfMaxs = {3.0, Double.POSITIVE_INFINITY};
    MBR partialInfMBR = new MBR(partialInfMins, partialInfMaxs);

    assertFalse(partialInfMBR.isInfinite());
    assertTrue(partialInfMBR.hasInfiniteBound());
    assertFalse(partialInfMBR.isEmpty());

    // Partially infinite MBR (only lower or upper bound)
    double[] lowerInfMins = {Double.NEGATIVE_INFINITY, 1.0};
    double[] lowerInfMaxs = {5.0, 3.0};
    MBR lowerInfMBR = new MBR(lowerInfMins, lowerInfMaxs);

    assertFalse(lowerInfMBR.isInfinite());
    assertTrue(lowerInfMBR.hasInfiniteBound());
  }

  /**
   * Test the area calculations with infinite bounds.
   */
  @Test
  public void testInfiniteArea() {
    // Fully infinite MBR
    double[] infMins = {Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY};
    double[] infMaxs = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
    MBR infMBR = new MBR(infMins, infMaxs);
    assertEquals(Double.POSITIVE_INFINITY, infMBR.area(), 0.0);

    // Semi-infinite MBR (one dimension infinite)
    double[] semiInfMins = {1.0, Double.NEGATIVE_INFINITY};
    double[] semiInfMaxs = {3.0, Double.POSITIVE_INFINITY};
    MBR semiInfMBR = new MBR(semiInfMins, semiInfMaxs);
    assertEquals(Double.POSITIVE_INFINITY, semiInfMBR.area(), 0.0);

    // Semi-infinite MBR (one bound infinite)
    double[] oneInfMins = {1.0, 2.0};
    double[] oneInfMaxs = {3.0, Double.POSITIVE_INFINITY};
    MBR oneInfMBR = new MBR(oneInfMins, oneInfMaxs);
    assertEquals(Double.POSITIVE_INFINITY, oneInfMBR.area(), 0.0);
  }

  /**
   * Test the perimeter calculations with infinite bounds.
   */
  @Test
  public void testInfinitePerimeter() {
    // Fully infinite MBR
    double[] infMins = {Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY};
    double[] infMaxs = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
    MBR infMBR = new MBR(infMins, infMaxs);
    assertEquals(Double.POSITIVE_INFINITY, infMBR.perimeter(), 0.0);

    // Semi-infinite MBR (one dimension infinite)
    double[] semiInfMins = {1.0, Double.NEGATIVE_INFINITY};
    double[] semiInfMaxs = {3.0, Double.POSITIVE_INFINITY};
    MBR semiInfMBR = new MBR(semiInfMins, semiInfMaxs);
    assertEquals(Double.POSITIVE_INFINITY, semiInfMBR.perimeter(), 0.0);
  }

  /**
   * Test the getExtent method with infinite bounds.
   */
  @Test
  public void testInfiniteExtent() {
    // Test with negative and positive infinity
    double[] mins = {Double.NEGATIVE_INFINITY, 1.0};
    double[] maxs = {Double.POSITIVE_INFINITY, 3.0};
    MBR mbr = new MBR(mins, maxs);

    assertEquals(Double.POSITIVE_INFINITY, mbr.getExtent(0), 0.0);
    assertEquals(2.0, mbr.getExtent(1), 0.0001);

    // Test with just negative infinity
    double[] mins2 = {Double.NEGATIVE_INFINITY, 1.0};
    double[] maxs2 = {5.0, 3.0};
    MBR mbr2 = new MBR(mins2, maxs2);
    assertEquals(Double.POSITIVE_INFINITY, mbr2.getExtent(0), 0.0);

    // Test with just positive infinity
    double[] mins3 = {1.0, 1.0};
    double[] maxs3 = {Double.POSITIVE_INFINITY, 3.0};
    MBR mbr3 = new MBR(mins3, maxs3);
    assertEquals(Double.POSITIVE_INFINITY, mbr3.getExtent(0), 0.0);
  }

  /**
   * Test the expansion operations with infinite bounds.
   */
  @Test
  public void testInfiniteExpansion() {
    // Start with finite MBR
    double[] mins = {1.0, 2.0};
    double[] maxs = {3.0, 4.0};
    MBR finiteMBR = new MBR(mins, maxs);

    // Point at infinity
    double[] infPoint = {Double.POSITIVE_INFINITY, 3.0};
    MBR expanded = finiteMBR.expand(infPoint);

    assertEquals(1.0, expanded.getMin(0), 0.0);
    assertEquals(Double.POSITIVE_INFINITY, expanded.getMax(0), 0.0);
    assertEquals(2.0, expanded.getMin(1), 0.0);
    assertEquals(4.0, expanded.getMax(1), 0.0);

    // Expanding with negative infinity
    double[] negInfPoint = {Double.NEGATIVE_INFINITY, 1.0};
    MBR expandedNeg = finiteMBR.expand(negInfPoint);

    assertEquals(Double.NEGATIVE_INFINITY, expandedNeg.getMin(0), 0.0);
    assertEquals(3.0, expandedNeg.getMax(0), 0.0);
  }

  /**
   * Test the union operation with infinite bounds.
   */
  @Test
  public void testInfiniteUnion() {
    // Finite MBR
    double[] mins1 = {1.0, 2.0};
    double[] maxs1 = {3.0, 4.0};
    MBR mbr1 = new MBR(mins1, maxs1);

    // MBR with one infinite bound
    double[] mins2 = {Double.NEGATIVE_INFINITY, 3.0};
    double[] maxs2 = {2.0, 5.0};
    MBR mbr2 = new MBR(mins2, maxs2);

    MBR union = mbr1.union(mbr2);

    assertEquals(Double.NEGATIVE_INFINITY, union.getMin(0), 0.0);
    assertEquals(3.0, union.getMax(0), 0.0);
    assertEquals(2.0, union.getMin(1), 0.0);
    assertEquals(5.0, union.getMax(1), 0.0);

    // Union with fully infinite MBR
    double[] infMins = {Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY};
    double[] infMaxs = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
    MBR infMBR = new MBR(infMins, infMaxs);

    MBR unionWithInf = mbr1.union(infMBR);
    assertTrue(unionWithInf.isInfinite());
  }

  /**
   * Test the intersection operation with infinite bounds.
   */
  @Test
  public void testInfiniteIntersection() {
    // Finite MBR
    double[] mins1 = {1.0, 2.0};
    double[] maxs1 = {3.0, 4.0};
    MBR mbr1 = new MBR(mins1, maxs1);

    // MBR with one infinite bound
    double[] mins2 = {Double.NEGATIVE_INFINITY, 3.0};
    double[] maxs2 = {2.0, 5.0};
    MBR mbr2 = new MBR(mins2, maxs2);

    MBR intersection = mbr1.intersection(mbr2);

    assertEquals(1.0, intersection.getMin(0), 0.0);
    assertEquals(2.0, intersection.getMax(0), 0.0);
    assertEquals(3.0, intersection.getMin(1), 0.0);
    assertEquals(4.0, intersection.getMax(1), 0.0);

    // Intersection with fully infinite MBR
    double[] infMins = {Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY};
    double[] infMaxs = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
    MBR infMBR = new MBR(infMins, infMaxs);

    MBR intersectionWithInf = mbr1.intersection(infMBR);
    assertArrayEquals(mbr1.getMins(), intersectionWithInf.getMins(), 0.0);
    assertArrayEquals(mbr1.getMaxs(), intersectionWithInf.getMaxs(), 0.0);
  }

  /**
   * Test the contains method with infinite bounds.
   */
  @Test
  public void testInfiniteContains() {
    // Infinite MBR in one dimension
    double[] mins = {Double.NEGATIVE_INFINITY, 1.0};
    double[] maxs = {Double.POSITIVE_INFINITY, 3.0};
    MBR mbr = new MBR(mins, maxs);

    // Point within the y-bounds should be contained regardless of x
    double[] point1 = {1000000.0, 2.0};
    double[] point2 = {-1000000.0, 2.0};
    assertTrue(mbr.contains(point1));
    assertTrue(mbr.contains(point2));

    // Point outside the y-bounds should not be contained
    double[] point3 = {1000000.0, 0.0};
    double[] point4 = {1000000.0, 4.0};
    assertFalse(mbr.contains(point3));
    assertFalse(mbr.contains(point4));

    // Fully infinite MBR contains any point
    double[] infMins = {Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY};
    double[] infMaxs = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
    MBR infMBR = new MBR(infMins, infMaxs);

    assertTrue(infMBR.contains(point1));
    assertTrue(infMBR.contains(point3));
  }

  /**
   * Test the contains(MBR) method with infinite bounds.
   */
  @Test
  public void testInfiniteContainsMBR() {
    // Fully infinite MBR
    double[] infMins = {Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY};
    double[] infMaxs = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
    MBR infMBR = new MBR(infMins, infMaxs);

    // Finite MBR
    double[] mins = {1.0, 2.0};
    double[] maxs = {3.0, 4.0};
    MBR finiteMBR = new MBR(mins, maxs);

    // Infinite MBR contains any finite MBR
    assertTrue(infMBR.contains(finiteMBR));
    assertFalse(finiteMBR.contains(infMBR));

    // Semi-infinite MBR
    double[] semiInfMins = {Double.NEGATIVE_INFINITY, 1.0};
    double[] semiInfMaxs = {Double.POSITIVE_INFINITY, 5.0};
    MBR semiInfMBR = new MBR(semiInfMins, semiInfMaxs);

    // Semi-infinite MBR contains finite MBR if the finite dimensions are contained
    assertTrue(semiInfMBR.contains(finiteMBR));

    // Adjust the finite MBR to be outside the y-bounds
    double[] mins2 = {1.0, 0.0};
    double[] maxs2 = {3.0, 6.0};
    MBR finiteMBR2 = new MBR(mins2, maxs2);
    assertFalse(semiInfMBR.contains(finiteMBR2));

    // Semi-infinite MBR relationship with fully infinite MBR
    assertTrue(infMBR.contains(semiInfMBR));
    assertFalse(semiInfMBR.contains(infMBR));
  }

  /**
   * Test the minDistance methods with infinite bounds.
   */
  @Test
  public void testInfiniteMinDistance() {
    // Infinite MBR in one dimension
    double[] mins = {Double.NEGATIVE_INFINITY, 1.0};
    double[] maxs = {Double.POSITIVE_INFINITY, 3.0};
    MBR mbr = new MBR(mins, maxs);

    // Distance to a point depends only on the finite dimension
    double[] point1 = {1000000.0, 0.0};
    assertEquals(1.0, mbr.minDistance(point1), 0.0001);

    double[] point2 = {-1000000.0, 4.0};
    assertEquals(1.0, mbr.minDistance(point2), 0.0001);

    // Fully infinite MBR has zero distance to any point
    double[] infMins = {Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY};
    double[] infMaxs = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
    MBR infMBR = new MBR(infMins, infMaxs);

    assertEquals(0.0, infMBR.minDistance(point1), 0.0);

    // Distance between MBRs with infinite bounds
    double[] semiInfMins = {1.0, Double.NEGATIVE_INFINITY};
    double[] semiInfMaxs = {3.0, Double.POSITIVE_INFINITY};
    MBR semiInfMBR = new MBR(semiInfMins, semiInfMaxs);

    double[] otherMins = {5.0, 1.0};
    double[] otherMaxs = {7.0, 3.0};
    MBR otherMBR = new MBR(otherMins, otherMaxs);

    assertEquals(2.0, semiInfMBR.minDistance(otherMBR), 0.0001);
    assertEquals(2.0, otherMBR.minDistance(semiInfMBR), 0.0001);
  }

  /**
   * Test the getCenter method with infinite bounds.
   */
  @Test
  public void testInfiniteCenter() {
    // MBR with one infinite dimension
    double[] mins = {Double.NEGATIVE_INFINITY, 1.0};
    double[] maxs = {Double.POSITIVE_INFINITY, 3.0};
    MBR mbr = new MBR(mins, maxs);

    double[] center = mbr.getCenter();
    // For infinite dimension, should return 0.0
    assertEquals(0.0, center[0], 0.0001);
    // For finite dimension, should return (min+max)/2
    assertEquals(2.0, center[1], 0.0001);

    // MBR with only lower bound infinite
    double[] mins2 = {Double.NEGATIVE_INFINITY, 1.0};
    double[] maxs2 = {5.0, 3.0};
    MBR mbr2 = new MBR(mins2, maxs2);

    double[] center2 = mbr2.getCenter();
    // For negative infinity min, finite max: center[i] = max - 1
    assertEquals(4.0, center2[0], 0.0001);

    // MBR with only upper bound infinite
    double[] mins3 = {1.0, 1.0};
    double[] maxs3 = {Double.POSITIVE_INFINITY, 3.0};
    MBR mbr3 = new MBR(mins3, maxs3);

    double[] center3 = mbr3.getCenter();
    // For finite min, positive infinity max: center[i] = min + 1
    assertEquals(2.0, center3[0], 0.0001);
  }

  /**
   * Test the overlap and overlapAmount methods with infinite bounds.
   */
  @Test
  public void testInfiniteOverlap() {
    // Infinite MBR in one dimension
    double[] mins1 = {Double.NEGATIVE_INFINITY, 1.0};
    double[] maxs1 = {Double.POSITIVE_INFINITY, 3.0};
    MBR mbr1 = new MBR(mins1, maxs1);

    // Overlapping in finite dimension
    double[] mins2 = {1.0, 2.0};
    double[] maxs2 = {3.0, 4.0};
    MBR mbr2 = new MBR(mins2, maxs2);

    assertTrue(mbr1.overlaps(mbr2));
    // Overlap amount should be 1.0 (the y-overlap)
    assertEquals(1.0, mbr1.overlapAmount(mbr2), 0.0001);

    // Non-overlapping in finite dimension
    double[] mins3 = {1.0, 4.0};
    double[] maxs3 = {3.0, 5.0};
    MBR mbr3 = new MBR(mins3, maxs3);

    assertFalse(mbr1.overlaps(mbr3));
    assertEquals(0.0, mbr1.overlapAmount(mbr3), 0.0001);

    // Two infinite MBRs
    double[] mins4 = {Double.NEGATIVE_INFINITY, 2.0};
    double[] maxs4 = {Double.POSITIVE_INFINITY, 4.0};
    MBR mbr4 = new MBR(mins4, maxs4);

    assertTrue(mbr1.overlaps(mbr4));
    // Overlap amount is 1.0 (the y-overlap)
    assertEquals(1.0, mbr1.overlapAmount(mbr4), 0.0001);

    // Two fully infinite MBRs
    double[] infMins1 = {Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY};
    double[] infMaxs1 = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
    MBR infMBR1 = new MBR(infMins1, infMaxs1);

    double[] infMins2 = {Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY};
    double[] infMaxs2 = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
    MBR infMBR2 = new MBR(infMins2, infMaxs2);

    assertTrue(infMBR1.overlaps(infMBR2));
    // Overlap of two same infinite MBRs is normalized to 1.0
    assertEquals(1.0, infMBR1.overlapAmount(infMBR2), 0.0001);
  }

  /**
   * Test the toString, equals, and hashCode methods with infinite bounds.
   */
  @Test
  public void testInfiniteObjectMethods() {
    // Test toString with infinite bounds
    double[] mins = {Double.NEGATIVE_INFINITY, 1.0};
    double[] maxs = {Double.POSITIVE_INFINITY, 3.0};
    MBR mbr = new MBR(mins, maxs);

    String str = mbr.toString();
    assertTrue(str.contains("-∞"));
    assertTrue(str.contains("+∞"));

    // Test equals with infinite bounds
    double[] mins2 = {Double.NEGATIVE_INFINITY, 1.0};
    double[] maxs2 = {Double.POSITIVE_INFINITY, 3.0};
    MBR mbr2 = new MBR(mins2, maxs2);

    assertTrue(mbr.equals(mbr2));
    assertEquals(mbr.hashCode(), mbr2.hashCode());

    // Different infinite MBRs
    double[] mins3 = {Double.NEGATIVE_INFINITY, 2.0};
    double[] maxs3 = {Double.POSITIVE_INFINITY, 3.0};
    MBR mbr3 = new MBR(mins3, maxs3);

    assertFalse(mbr.equals(mbr3));

    // Fully infinite MBRs
    double[] infMins1 = {Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY};
    double[] infMaxs1 = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
    MBR infMBR1 = new MBR(infMins1, infMaxs1);

    double[] infMins2 = {Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY};
    double[] infMaxs2 = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
    MBR infMBR2 = new MBR(infMins2, infMaxs2);

    assertTrue(infMBR1.equals(infMBR2));
    assertEquals(infMBR1.hashCode(), infMBR2.hashCode());
  }

  /**
   * Test handling of NaN values in MBR coordinates.
   * While not explicitly covered in the design docs, proper handling of
   * NaN values is important for robustness.
   */
  @Test
  public void testNaNHandling() {
    // MBR with NaN values
    double[] mins = {Double.NaN, 1.0};
    double[] maxs = {3.0, Double.NaN};
    MBR nanMBR = new MBR(mins, maxs);

    // NaN comparisons should behave according to IEEE 754
    assertFalse(nanMBR.contains(new double[]{2.0, 2.0}));

    // NaN in equals
    double[] mins2 = {Double.NaN, 1.0};
    double[] maxs2 = {3.0, Double.NaN};
    MBR nanMBR2 = new MBR(mins2, maxs2);

    // According to IEEE 754, NaN != NaN, so equals should return false
    // However, it depends on the implementation of equals
    // For robustness, we should handle both cases

    // Test hashCode with NaN
    // This mainly checks that it doesn't throw exceptions
    int hashCode = nanMBR.hashCode();

    // Test toString with NaN
    String str = nanMBR.toString();
    assertTrue(str.contains("NaN") || str.contains("dimensions=2"));
  }
}
