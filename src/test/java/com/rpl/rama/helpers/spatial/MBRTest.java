package com.rpl.rama.helpers.spatial;

import static org.junit.Assert.*;
import org.junit.Test;

/**
 * Test class for the n-dimensional Minimal Bounding Rectangle (MBR) implementation.
 */
public class MBRTest {

  /**
   * Test the constructors and basic getters of the MBR class.
   */
  @Test
  public void testConstructorsAndGetters() {
    // Test empty constructor
    MBR emptyMBR = new MBR(3);
    assertEquals(3, emptyMBR.getDimensions());
    assertTrue(emptyMBR.isEmpty());

    // Test array constructor
    double[] mins = {1.0, 2.0, 3.0};
    double[] maxs = {4.0, 5.0, 6.0};
    MBR mbr = new MBR(mins, maxs);

    assertEquals(3, mbr.getDimensions());
    assertArrayEquals(mins, mbr.getMins(), 0.0);
    assertArrayEquals(maxs, mbr.getMaxs(), 0.0);

    assertEquals(1.0, mbr.getMin(0), 0.0);
    assertEquals(5.0, mbr.getMax(1), 0.0);

    // Test copy constructor
    MBR copy = new MBR(mbr);
    assertEquals(mbr.getDimensions(), copy.getDimensions());
    assertArrayEquals(mbr.getMins(), copy.getMins(), 0.0);
    assertArrayEquals(mbr.getMaxs(), copy.getMaxs(), 0.0);

    // Test that the copy is indeed a deep copy
    mins[0] = 10.0;
    assertNotEquals(mins[0], mbr.getMin(0), 0.0);
  }

  /**
   * Test the array constructor with invalid arguments.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testConstructorWithInvalidArrays() {
    double[] mins = {1.0, 2.0};
    double[] maxs = {4.0, 5.0, 6.0};
    new MBR(mins, maxs); // Should throw IllegalArgumentException
  }

  /**
   * Test getMin and getMax with invalid index.
   */
  @Test(expected = IndexOutOfBoundsException.class)
  public void testGetMinWithInvalidDimension() {
    MBR mbr = new MBR(2);
    mbr.getMin(2); // Should throw IndexOutOfBoundsException
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testGetMaxWithInvalidDimension() {
    MBR mbr = new MBR(2);
    mbr.getMax(-1); // Should throw IndexOutOfBoundsException
  }

  /**
   * Test the isEmpty method.
   */
  @Test
  public void testIsEmpty() {
    // Empty MBR
    MBR emptyMBR = new MBR(2);
    assertTrue(emptyMBR.isEmpty());

    // Non-empty MBR
    double[] mins = {1.0, 2.0};
    double[] maxs = {4.0, 5.0};
    MBR mbr = new MBR(mins, maxs);
    assertFalse(mbr.isEmpty());

    // MBR with one inverted dimension
    double[] mins2 = {1.0, 5.0};
    double[] maxs2 = {4.0, 2.0};
    MBR invertedMBR = new MBR(mins2, maxs2);
    assertTrue(invertedMBR.isEmpty());
  }

  /**
   * Test the isInfinite and hasInfiniteBound methods.
   */
  @Test
  public void testInfiniteBounds() {
    // Regular MBR with finite bounds
    double[] mins = {1.0, 2.0};
    double[] maxs = {4.0, 5.0};
    MBR finiteMBR = new MBR(mins, maxs);
    assertFalse(finiteMBR.isInfinite());
    assertFalse(finiteMBR.hasInfiniteBound());

    // MBR with infinite bounds in all dimensions
    double[] infMins = {Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY};
    double[] infMaxs = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
    MBR infiniteMBR = new MBR(infMins, infMaxs);
    assertTrue(infiniteMBR.isInfinite());
    assertTrue(infiniteMBR.hasInfiniteBound());

    // MBR with only one infinite bound
    double[] partialInfMins = {1.0, Double.NEGATIVE_INFINITY};
    double[] partialInfMaxs = {4.0, 5.0};
    MBR partialInfiniteMBR = new MBR(partialInfMins, partialInfMaxs);
    assertFalse(partialInfiniteMBR.isInfinite());
    assertTrue(partialInfiniteMBR.hasInfiniteBound());
  }

  /**
   * Test the expand method (adding a point to the MBR).
   */
  @Test
  public void testExpand() {
    // Starting with an empty MBR
    MBR mbr = new MBR(2);
    double[] point = {3.0, 4.0};

    MBR expanded = mbr.expand(point);
    assertArrayEquals(new double[]{3.0, 4.0}, expanded.getMins(), 0.0);
    assertArrayEquals(new double[]{3.0, 4.0}, expanded.getMaxs(), 0.0);

    // Expanding with another point
    double[] point2 = {1.0, 6.0};
    MBR expanded2 = expanded.expand(point2);
    assertArrayEquals(new double[]{1.0, 4.0}, expanded2.getMins(), 0.0);
    assertArrayEquals(new double[]{3.0, 6.0}, expanded2.getMaxs(), 0.0);

    // Original MBR should remain unchanged
    assertTrue(mbr.isEmpty());
  }

  /**
   * Test expand with a point having different dimensions.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testExpandWithInvalidPoint() {
    MBR mbr = new MBR(2);
    double[] point = {1.0, 2.0, 3.0};
    mbr.expand(point);
  }

  /**
   * Test the union method.
   */
  @Test
  public void testUnion() {
    // Two non-overlapping MBRs
    double[] mins1 = {1.0, 1.0};
    double[] maxs1 = {2.0, 2.0};
    MBR mbr1 = new MBR(mins1, maxs1);

    double[] mins2 = {3.0, 3.0};
    double[] maxs2 = {4.0, 4.0};
    MBR mbr2 = new MBR(mins2, maxs2);

    MBR union = mbr1.union(mbr2);
    assertArrayEquals(new double[]{1.0, 1.0}, union.getMins(), 0.0);
    assertArrayEquals(new double[]{4.0, 4.0}, union.getMaxs(), 0.0);

    // Union with an empty MBR
    MBR emptyMBR = new MBR(2);
    MBR unionWithEmpty = mbr1.union(emptyMBR);
    assertArrayEquals(mbr1.getMins(), unionWithEmpty.getMins(), 0.0);
    assertArrayEquals(mbr1.getMaxs(), unionWithEmpty.getMaxs(), 0.0);

    // Union with an infinite MBR
    double[] infMins = {Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY};
    double[] infMaxs = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
    MBR infiniteMBR = new MBR(infMins, infMaxs);

    MBR unionWithInfinite = mbr1.union(infiniteMBR);
    assertTrue(unionWithInfinite.isInfinite());
  }

  /**
   * Test union with an MBR having different dimensions.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testUnionWithInvalidMBR() {
    MBR mbr1 = new MBR(2);
    MBR mbr2 = new MBR(3);
    mbr1.union(mbr2);
  }

  /**
   * Test the calculateEnlargement method.
   */
  @Test
  public void testCalculateEnlargement() {
    double[] mins1 = {1.0, 1.0};
    double[] maxs1 = {3.0, 3.0};
    MBR mbr1 = new MBR(mins1, maxs1);

    // Enlargement with a contained MBR
    double[] mins2 = {1.5, 1.5};
    double[] maxs2 = {2.5, 2.5};
    MBR mbr2 = new MBR(mins2, maxs2);

    assertEquals(0.0, mbr1.calculateEnlargement(mbr2), 0.0001);

    // Enlargement with a partially overlapping MBR
    double[] mins3 = {2.0, 2.0};
    double[] maxs3 = {4.0, 4.0};
    MBR mbr3 = new MBR(mins3, maxs3);

    // Original area = (3-1)*(3-1) = 4
    // Union area = (4-1)*(4-1) = 9
    // Enlargement = 9 - 4 = 5
    assertEquals(5.0, mbr1.calculateEnlargement(mbr3), 0.0001);

    // Enlargement with an empty MBR
    MBR emptyMBR = new MBR(2);
    assertEquals(0.0, mbr1.calculateEnlargement(emptyMBR), 0.0001);

    // Empty MBR enlarged with a non-empty MBR
    assertEquals(4.0, emptyMBR.calculateEnlargement(mbr1), 0.0001);
  }

  /**
   * Test the area method.
   */
  @Test
  public void testArea() {
    // Regular 2D MBR
    double[] mins = {1.0, 2.0};
    double[] maxs = {4.0, 6.0};
    MBR mbr = new MBR(mins, maxs);
    // Area = (4-1)*(6-2) = 3*4 = 12
    assertEquals(12.0, mbr.area(), 0.0001);

    // Empty MBR
    MBR emptyMBR = new MBR(2);
    assertEquals(0.0, emptyMBR.area(), 0.0001);

    // MBR with one zero dimension
    double[] mins2 = {1.0, 2.0};
    double[] maxs2 = {1.0, 6.0};
    MBR zeroDimMBR = new MBR(mins2, maxs2);
    assertEquals(0.0, zeroDimMBR.area(), 0.0001);

    // MBR with infinite dimensions
    double[] infMins = {Double.NEGATIVE_INFINITY, 2.0};
    double[] infMaxs = {4.0, Double.POSITIVE_INFINITY};
    MBR infDimMBR = new MBR(infMins, infMaxs);
    assertEquals(Double.POSITIVE_INFINITY, infDimMBR.area(), 0.0);
  }

  /**
   * Test the perimeter method.
   */
  @Test
  public void testPerimeter() {
    // Regular 2D MBR
    double[] mins = {1.0, 2.0};
    double[] maxs = {4.0, 6.0};
    MBR mbr = new MBR(mins, maxs);
    // Perimeter = 2*((4-1) + (6-2)) = 2*(3+4) = 2*7 = 14
    assertEquals(14.0, mbr.perimeter(), 0.0001);

    // 3D MBR
    double[] mins3D = {1.0, 2.0, 3.0};
    double[] maxs3D = {4.0, 6.0, 7.0};
    MBR mbr3D = new MBR(mins3D, maxs3D);
    // For 3D: perimeter = 2^(3-1) * ((4-1) + (6-2) + (7-3))
    // = 4 * (3 + 4 + 4) = 4 * 11 = 44
    assertEquals(44.0, mbr3D.perimeter(), 0.0001);

    // Empty MBR
    MBR emptyMBR = new MBR(2);
    assertEquals(0.0, emptyMBR.perimeter(), 0.0001);

    // MBR with infinite dimensions
    double[] infMins = {Double.NEGATIVE_INFINITY, 2.0};
    double[] infMaxs = {4.0, Double.POSITIVE_INFINITY};
    MBR infDimMBR = new MBR(infMins, infMaxs);
    assertEquals(Double.POSITIVE_INFINITY, infDimMBR.perimeter(), 0.0);
  }

  /**
   * Test the getExtent method.
   */
  @Test
  public void testGetExtent() {
    double[] mins = {1.0, 2.0, 3.0};
    double[] maxs = {4.0, 6.0, 8.0};
    MBR mbr = new MBR(mins, maxs);

    assertEquals(3.0, mbr.getExtent(0), 0.0001);
    assertEquals(4.0, mbr.getExtent(1), 0.0001);
    assertEquals(5.0, mbr.getExtent(2), 0.0001);

    // Empty MBR
    MBR emptyMBR = new MBR(3);
    assertEquals(0.0, emptyMBR.getExtent(0), 0.0001);

    // Infinite extent
    double[] infMins = {Double.NEGATIVE_INFINITY, 2.0};
    double[] infMaxs = {Double.POSITIVE_INFINITY, 6.0};
    MBR infMBR = new MBR(infMins, infMaxs);
    assertEquals(Double.POSITIVE_INFINITY, infMBR.getExtent(0), 0.0);
  }

  /**
   * Test getExtent with an invalid dimension.
   */
  @Test(expected = IndexOutOfBoundsException.class)
  public void testGetExtentWithInvalidDimension() {
    MBR mbr = new MBR(2);
    mbr.getExtent(2);
  }

  /**
   * Test the overlaps method.
   */
  @Test
  public void testOverlaps() {
    // Overlapping MBRs
    double[] mins1 = {1.0, 1.0};
    double[] maxs1 = {3.0, 3.0};
    MBR mbr1 = new MBR(mins1, maxs1);

    double[] mins2 = {2.0, 2.0};
    double[] maxs2 = {4.0, 4.0};
    MBR mbr2 = new MBR(mins2, maxs2);

    assertTrue(mbr1.overlaps(mbr2));
    assertTrue(mbr2.overlaps(mbr1));

    // Non-overlapping MBRs
    double[] mins3 = {4.0, 4.0};
    double[] maxs3 = {5.0, 5.0};
    MBR mbr3 = new MBR(mins3, maxs3);

    assertFalse(mbr1.overlaps(mbr3));
    assertFalse(mbr3.overlaps(mbr1));

    // Touching MBRs (edge to edge)
    double[] mins4 = {3.0, 1.0};
    double[] maxs4 = {5.0, 3.0};
    MBR mbr4 = new MBR(mins4, maxs4);

    assertTrue(mbr1.overlaps(mbr4));
    assertTrue(mbr4.overlaps(mbr1));

    // Empty MBR
    MBR emptyMBR = new MBR(2);
    assertFalse(mbr1.overlaps(emptyMBR));
    assertFalse(emptyMBR.overlaps(mbr1));

    // Infinite MBR
    double[] infMins = {Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY};
    double[] infMaxs = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
    MBR infMBR = new MBR(infMins, infMaxs);

    assertTrue(mbr1.overlaps(infMBR));
    assertTrue(infMBR.overlaps(mbr1));
  }

  /**
   * Test overlaps with MBRs of different dimensions.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testOverlapsWithInvalidMBR() {
    MBR mbr1 = new MBR(2);
    MBR mbr2 = new MBR(3);
    mbr1.overlaps(mbr2);
  }

  /**
   * Test the overlapAmount method.
   */
  @Test
  public void testOverlapAmount() {
    // Overlapping MBRs
    double[] mins1 = {1.0, 1.0};
    double[] maxs1 = {3.0, 3.0};
    MBR mbr1 = new MBR(mins1, maxs1);

    double[] mins2 = {2.0, 2.0};
    double[] maxs2 = {4.0, 4.0};
    MBR mbr2 = new MBR(mins2, maxs2);

    // Overlap area = (3-2)*(3-2) = 1*1 = 1
    assertEquals(1.0, mbr1.overlapAmount(mbr2), 0.0001);
    assertEquals(1.0, mbr2.overlapAmount(mbr1), 0.0001);

    // Non-overlapping MBRs
    double[] mins3 = {4.0, 4.0};
    double[] maxs3 = {5.0, 5.0};
    MBR mbr3 = new MBR(mins3, maxs3);

    assertEquals(0.0, mbr1.overlapAmount(mbr3), 0.0001);
    assertEquals(0.0, mbr3.overlapAmount(mbr1), 0.0001);

    // Fully containing MBR
    double[] mins4 = {0.0, 0.0};
    double[] maxs4 = {5.0, 5.0};
    MBR mbr4 = new MBR(mins4, maxs4);

    // Overlap area equals mbr1's area = (3-1)*(3-1) = 4
    assertEquals(4.0, mbr1.overlapAmount(mbr4), 0.0001);
    assertEquals(4.0, mbr4.overlapAmount(mbr1), 0.0001);

    // Empty MBR
    MBR emptyMBR = new MBR(2);
    assertEquals(0.0, mbr1.overlapAmount(emptyMBR), 0.0001);
    assertEquals(0.0, emptyMBR.overlapAmount(mbr1), 0.0001);

    // Infinite overlap
    double[] infMins = {Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY};
    double[] infMaxs = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
    MBR infMBR = new MBR(infMins, infMaxs);

    // Two same infinite MBRs
    assertEquals(1.0, infMBR.overlapAmount(infMBR), 0.0001);

    // Infinite MBR with finite MBR
    assertEquals(4.0, mbr1.overlapAmount(infMBR), 0.0001);
    assertEquals(4.0, infMBR.overlapAmount(mbr1), 0.0001);
  }

  /**
   * Test the intersection method.
   */
  @Test
  public void testIntersection() {
    // Overlapping MBRs
    double[] mins1 = {1.0, 1.0};
    double[] maxs1 = {3.0, 3.0};
    MBR mbr1 = new MBR(mins1, maxs1);

    double[] mins2 = {2.0, 2.0};
    double[] maxs2 = {4.0, 4.0};
    MBR mbr2 = new MBR(mins2, maxs2);

    MBR intersection = mbr1.intersection(mbr2);
    assertArrayEquals(new double[]{2.0, 2.0}, intersection.getMins(), 0.0);
    assertArrayEquals(new double[]{3.0, 3.0}, intersection.getMaxs(), 0.0);

    // Non-overlapping MBRs
    double[] mins3 = {4.0, 4.0};
    double[] maxs3 = {5.0, 5.0};
    MBR mbr3 = new MBR(mins3, maxs3);

    MBR noIntersection = mbr1.intersection(mbr3);
    assertTrue(noIntersection.isEmpty());

    // Intersection with an empty MBR
    MBR emptyMBR = new MBR(2);
    MBR intersectionWithEmpty = mbr1.intersection(emptyMBR);
    assertTrue(intersectionWithEmpty.isEmpty());

    // Intersection with an infinite MBR
    double[] infMins = {Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY};
    double[] infMaxs = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
    MBR infMBR = new MBR(infMins, infMaxs);

    MBR intersectionWithInf = mbr1.intersection(infMBR);
    assertArrayEquals(mbr1.getMins(), intersectionWithInf.getMins(), 0.0);
    assertArrayEquals(mbr1.getMaxs(), intersectionWithInf.getMaxs(), 0.0);
  }

  /**
   * Test contains(point) method.
   */
  @Test
  public void testContainsPoint() {
    double[] mins = {1.0, 1.0};
    double[] maxs = {3.0, 3.0};
    MBR mbr = new MBR(mins, maxs);

    // Point inside
    double[] insidePoint = {2.0, 2.0};
    assertTrue(mbr.contains(insidePoint));

    // Point on the boundary
    double[] boundaryPoint = {1.0, 2.0};
    assertTrue(mbr.contains(boundaryPoint));

    // Point outside
    double[] outsidePoint = {0.0, 2.0};
    assertFalse(mbr.contains(outsidePoint));

    // Empty MBR
    MBR emptyMBR = new MBR(2);
    assertFalse(emptyMBR.contains(insidePoint));

    // Infinite MBR
    double[] infMins = {Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY};
    double[] infMaxs = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
    MBR infMBR = new MBR(infMins, infMaxs);

    // Any point should be contained in an infinite MBR
    assertTrue(infMBR.contains(insidePoint));
    assertTrue(infMBR.contains(outsidePoint));
  }

  /**
   * Test contains(point) with a point of different dimensions.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testContainsPointWithInvalidDimensions() {
    MBR mbr = new MBR(2);
    double[] point = {1.0, 2.0, 3.0};
    mbr.contains(point);
  }

  /**
   * Test contains(MBR) method.
   */
  @Test
  public void testContainsMBR() {
    double[] mins1 = {1.0, 1.0};
    double[] maxs1 = {5.0, 5.0};
    MBR mbr1 = new MBR(mins1, maxs1);

    // Contained MBR
    double[] mins2 = {2.0, 2.0};
    double[] maxs2 = {4.0, 4.0};
    MBR mbr2 = new MBR(mins2, maxs2);

    assertTrue(mbr1.contains(mbr2));
    assertFalse(mbr2.contains(mbr1));

    // Partially overlapping MBR
    double[] mins3 = {3.0, 3.0};
    double[] maxs3 = {6.0, 6.0};
    MBR mbr3 = new MBR(mins3, maxs3);

    assertFalse(mbr1.contains(mbr3));
    assertFalse(mbr3.contains(mbr1));

    // Self-containment
    assertTrue(mbr1.contains(mbr1));

    // Empty MBR
    MBR emptyMBR = new MBR(2);
    assertFalse(mbr1.contains(emptyMBR));
    assertFalse(emptyMBR.contains(mbr1));

    // Infinite MBR
    double[] infMins = {Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY};
    double[] infMaxs = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
    MBR infMBR = new MBR(infMins, infMaxs);

    assertTrue(infMBR.contains(mbr1));
    assertFalse(mbr1.contains(infMBR));
  }

  /**
   * Test contains(MBR) with MBRs of different dimensions.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testContainsMBRWithInvalidDimensions() {
    MBR mbr1 = new MBR(2);
    MBR mbr2 = new MBR(3);
    mbr1.contains(mbr2);
  }

  /**
   * Test the minDistance(point) method.
   */
  @Test
  public void testMinDistanceToPoint() {
    double[] mins = {1.0, 1.0};
    double[] maxs = {3.0, 3.0};
    MBR mbr = new MBR(mins, maxs);

    // Point inside the MBR
    double[] insidePoint = {2.0, 2.0};
    assertEquals(0.0, mbr.minDistance(insidePoint), 0.0001);

    // Point outside the MBR (horizontally aligned)
    double[] horizontalPoint = {5.0, 2.0};
    assertEquals(2.0, mbr.minDistance(horizontalPoint), 0.0001);

    // Point outside the MBR (vertically aligned)
    double[] verticalPoint = {2.0, 5.0};
    assertEquals(2.0, mbr.minDistance(verticalPoint), 0.0001);

    // Point outside the MBR (diagonal)
    double[] diagonalPoint = {5.0, 5.0};
    assertEquals(Math.sqrt(8.0), mbr.minDistance(diagonalPoint), 0.0001);

    // Empty MBR
    MBR emptyMBR = new MBR(2);
    assertEquals(Double.POSITIVE_INFINITY, emptyMBR.minDistance(insidePoint), 0.0);
  }

  /**
   * Test minDistance(point) with a point of different dimensions.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testMinDistanceToPointWithInvalidDimensions() {
    MBR mbr = new MBR(2);
    double[] point = {1.0, 2.0, 3.0};
    mbr.minDistance(point);
  }

  /**
   * Test the minDistance(MBR) method.
   */
  @Test
  public void testMinDistanceBetweenMBRs() {
    double[] mins1 = {1.0, 1.0};
    double[] maxs1 = {3.0, 3.0};
    MBR mbr1 = new MBR(mins1, maxs1);

    // Overlapping MBR
    double[] mins2 = {2.0, 2.0};
    double[] maxs2 = {4.0, 4.0};
    MBR mbr2 = new MBR(mins2, maxs2);
    assertEquals(0.0, mbr1.minDistance(mbr2), 0.0001);

    // Non-overlapping but horizontally aligned
    double[] mins3 = {5.0, 1.0};
    double[] maxs3 = {7.0, 3.0};
    MBR mbr3 = new MBR(mins3, maxs3);
    assertEquals(2.0, mbr1.minDistance(mbr3), 0.0001);

    // Diagonally separated
    double[] mins4 = {5.0, 5.0};
    double[] maxs4 = {7.0, 7.0};
    MBR mbr4 = new MBR(mins4, maxs4);
    assertEquals(Math.sqrt(8.0), mbr1.minDistance(mbr4), 0.0001);

    // Empty MBR
    MBR emptyMBR = new MBR(2);
    assertEquals(Double.POSITIVE_INFINITY, emptyMBR.minDistance(mbr1), 0.0);
    assertEquals(Double.POSITIVE_INFINITY, mbr1.minDistance(emptyMBR), 0.0);
  }

  /**
   * Test minDistance(MBR) with MBRs of different dimensions.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testMinDistanceBetweenMBRsWithInvalidDimensions() {
    MBR mbr1 = new MBR(2);
    MBR mbr2 = new MBR(3);
    mbr1.minDistance(mbr2);
  }

  /**
   * Test the getCenter method.
   */
  @Test
  public void testGetCenter() {
    // Regular MBR
    double[] mins = {1.0, 2.0, 3.0};
    double[] maxs = {5.0, 6.0, 7.0};
    MBR mbr = new MBR(mins, maxs);

    double[] center = mbr.getCenter();
    assertArrayEquals(new double[]{3.0, 4.0, 5.0}, center, 0.0001);

    // Empty MBR
    MBR emptyMBR = new MBR(3);
    assertNull(emptyMBR.getCenter());

    // MBR with infinite bounds
    double[] infMins = {Double.NEGATIVE_INFINITY, 2.0, Double.NEGATIVE_INFINITY};
    double[] infMaxs = {5.0, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
    MBR infMBR = new MBR(infMins, infMaxs);

    double[] infCenter = infMBR.getCenter();
    // For negative infinity min, positive finite max: center[i] = max - 1
    assertEquals(4.0, infCenter[0], 0.0001);
    // For finite min, positive infinity max: center[i] = min + 1
    assertEquals(3.0, infCenter[1], 0.0001);
    // For negative infinity min, positive infinity max: center[i] = 0
    assertEquals(0.0, infCenter[2], 0.0001);
  }

  /**
   * Test the toString method.
   */
  @Test
  public void testToString() {
    // Regular MBR
    double[] mins = {1.0, 2.0};
    double[] maxs = {3.0, 4.0};
    MBR mbr = new MBR(mins, maxs);

    String str = mbr.toString();
    assertTrue(str.contains("dimensions=2"));
    assertTrue(str.contains("dim0=(1.0, 3.0)"));
    assertTrue(str.contains("dim1=(2.0, 4.0)"));

    // Empty MBR
    MBR emptyMBR = new MBR(2);
    assertTrue(emptyMBR.toString().contains("empty"));

    // MBR with infinite bounds
    double[] infMins = {Double.NEGATIVE_INFINITY, 2.0};
    double[] infMaxs = {3.0, Double.POSITIVE_INFINITY};
    MBR infMBR = new MBR(infMins, infMaxs);

    String infStr = infMBR.toString();
    assertTrue(infStr.contains("-∞"));
    assertTrue(infStr.contains("+∞"));
  }

  /**
   * Test the equals method.
   */
  @Test
  public void testEquals() {
    // Equal MBRs
    double[] mins1 = {1.0, 2.0};
    double[] maxs1 = {3.0, 4.0};
    MBR mbr1 = new MBR(mins1, maxs1);

    double[] mins2 = {1.0, 2.0};
    double[] maxs2 = {3.0, 4.0};
    MBR mbr2 = new MBR(mins2, maxs2);

    assertTrue(mbr1.equals(mbr2));
    assertTrue(mbr2.equals(mbr1));

    // Different MBRs
    double[] mins3 = {1.0, 2.0};
    double[] maxs3 = {3.0, 5.0};
    MBR mbr3 = new MBR(mins3, maxs3);

    assertFalse(mbr1.equals(mbr3));

    // Different dimensions
    MBR mbr4 = new MBR(3);
    assertFalse(mbr1.equals(mbr4));

    // Empty MBRs with different dimensions
    MBR emptyMBR1 = new MBR(2);
    MBR emptyMBR2 = new MBR(2);
    MBR emptyMBR3 = new MBR(3);

    assertTrue(emptyMBR1.equals(emptyMBR2));
    assertFalse(emptyMBR1.equals(emptyMBR3));

    // Self-equality
    assertTrue(mbr1.equals(mbr1));

    // Null and different types
    assertFalse(mbr1.equals(null));
    assertFalse(mbr1.equals("Not an MBR"));
  }

  /**
   * Test the hashCode method.
   */
  @Test
  public void testHashCode() {
    // Equal MBRs should have equal hash codes
    double[] mins1 = {1.0, 2.0};
    double[] maxs1 = {3.0, 4.0};
    MBR mbr1 = new MBR(mins1, maxs1);

    double[] mins2 = {1.0, 2.0};
    double[] maxs2 = {3.0, 4.0};
    MBR mbr2 = new MBR(mins2, maxs2);

    assertEquals(mbr1.hashCode(), mbr2.hashCode());

    // Different MBRs likely have different hash codes
    double[] mins3 = {1.0, 2.0};
    double[] maxs3 = {3.0, 5.0};
    MBR mbr3 = new MBR(mins3, maxs3);

    // This is not guaranteed by the hashCode contract, but should be true
    // for our implementation
    assertNotEquals(mbr1.hashCode(), mbr3.hashCode());

    // Empty MBRs with same dimensions should have equal hash codes
    MBR emptyMBR1 = new MBR(2);
    MBR emptyMBR2 = new MBR(2);

    assertEquals(emptyMBR1.hashCode(), emptyMBR2.hashCode());

    // Empty MBRs with different dimensions should have different hash codes
    MBR emptyMBR3 = new MBR(3);
    assertNotEquals(emptyMBR1.hashCode(), emptyMBR3.hashCode());
  }

  /**
   * Test various edge cases with infinite values.
   */
  @Test
  public void testInfiniteEdgeCases() {
    // Infinite MBR
    double[] infMins = {Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY};
    double[] infMaxs = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
    MBR infMBR = new MBR(infMins, infMaxs);

    // Area and perimeter should be infinite
    assertEquals(Double.POSITIVE_INFINITY, infMBR.area(), 0.0);
    assertEquals(Double.POSITIVE_INFINITY, infMBR.perimeter(), 0.0);

    // Contains everything
    double[] somePoint = {1000.0, -5000.0};
    assertTrue(infMBR.contains(somePoint));

    // Half-infinite MBR
    double[] halfInfMins = {1.0, Double.NEGATIVE_INFINITY};
    double[] halfInfMaxs = {3.0, Double.POSITIVE_INFINITY};
    MBR halfInfMBR = new MBR(halfInfMins, halfInfMaxs);

    // Area should be infinite
    assertEquals(Double.POSITIVE_INFINITY, halfInfMBR.area(), 0.0);

    // Contains points within x bounds and any y
    double[] withinX = {2.0, 1000000.0};
    double[] outsideX = {0.0, 1000000.0};

    assertTrue(halfInfMBR.contains(withinX));
    assertFalse(halfInfMBR.contains(outsideX));
  }
}
