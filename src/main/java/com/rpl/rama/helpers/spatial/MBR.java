package com.rpl.rama.helpers.spatial;

import com.rpl.rama.RamaSerializable;

/**
 * An n-dimensional Minimal Bounding Rectangle (MBR) implementation.
 *
 * This class uses min/max coordinate representation and supports open/unbounded ranges
 * using Double.NEGATIVE_INFINITY and Double.POSITIVE_INFINITY. The implementation follows
 * the design decisions documented in MBR-representation.md and MBR-open-range.md.
 */
public class MBR implements RamaSerializable {
  private final double[] mins;
  private final double[] maxs;
  private final int dimensions;

  /**
   * Creates an empty MBR with the specified number of dimensions.
   * An empty MBR has mins set to POSITIVE_INFINITY and maxs set to NEGATIVE_INFINITY.
   *
   * @param dimensions The number of dimensions for this MBR
   */
  public MBR(int dimensions) {
    this.dimensions = dimensions;
    this.mins = new double[dimensions];
    this.maxs = new double[dimensions];

    // Initialize as empty MBR
    for (int i = 0; i < dimensions; i++) {
      mins[i] = Double.POSITIVE_INFINITY;
      maxs[i] = Double.NEGATIVE_INFINITY;
    }
  }

  /**
   * Creates an MBR with the specified min and max coordinates.
   *
   * @param mins Minimum coordinates for each dimension
   * @param maxs Maximum coordinates for each dimension
   * @throws IllegalArgumentException If the arrays have different lengths
   */
  public MBR(double[] mins, double[] maxs) {
    if (mins.length != maxs.length) {
      throw new IllegalArgumentException("Min and max arrays must have the same length");
    }

    this.dimensions = mins.length;
    this.mins = mins.clone();
    this.maxs = maxs.clone();
  }

  /**
   * Copy constructor to create a new MBR from an existing one.
   *
   * @param other The MBR to copy
   */
  public MBR(MBR other) {
    this.dimensions = other.dimensions;
    this.mins = other.mins.clone();
    this.maxs = other.maxs.clone();
  }

  /**
   * Returns the number of dimensions of this MBR.
   *
   * @return The number of dimensions
   */
  public int getDimensions() {
    return dimensions;
  }

  /**
   * Returns the minimum coordinate for the specified dimension.
   *
   * @param dimension The dimension index (0-based)
   * @return The minimum coordinate value
   * @throws IndexOutOfBoundsException If the dimension is invalid
   */
  public double getMin(int dimension) {
    if (dimension < 0 || dimension >= dimensions) {
      throw new IndexOutOfBoundsException("Invalid dimension: " + dimension);
    }
    return mins[dimension];
  }

  /**
   * Returns the maximum coordinate for the specified dimension.
   *
   * @param dimension The dimension index (0-based)
   * @return The maximum coordinate value
   * @throws IndexOutOfBoundsException If the dimension is invalid
   */
  public double getMax(int dimension) {
    if (dimension < 0 || dimension >= dimensions) {
      throw new IndexOutOfBoundsException("Invalid dimension: " + dimension);
    }
    return maxs[dimension];
  }

  /**
   * Returns an array containing all minimum coordinates.
   *
   * @return A copy of the minimum coordinates array
   */
  public double[] getMins() {
    return mins.clone();
  }

  /**
   * Returns an array containing all maximum coordinates.
   *
   * @return A copy of the maximum coordinates array
   */
  public double[] getMaxs() {
    return maxs.clone();
  }

  /**
   * Checks if this MBR is empty.
   * An MBR is considered empty if for any dimension, min > max.
   *
   * @return true if this MBR is empty, false otherwise
   */
  public boolean isEmpty() {
    for (int i = 0; i < dimensions; i++) {
      if (mins[i] > maxs[i]) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks if this MBR is infinite in all dimensions.
   *
   * @return true if this MBR is infinite in all dimensions, false otherwise
   */
  public boolean isInfinite() {
    for (int i = 0; i < dimensions; i++) {
      if (mins[i] != Double.NEGATIVE_INFINITY || maxs[i] != Double.POSITIVE_INFINITY) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks if this MBR has at least one infinite bound.
   *
   * @return true if at least one dimension has an infinite bound, false otherwise
   */
  public boolean hasInfiniteBound() {
    for (int i = 0; i < dimensions; i++) {
      if (mins[i] == Double.NEGATIVE_INFINITY || maxs[i] == Double.POSITIVE_INFINITY) {
        return true;
      }
    }
    return false;
  }

  /**
   * Creates a new MBR that is the union of this MBR and the specified point.
   *
   * @param point The point to include in the MBR
   * @return A new MBR that contains both this MBR and the point
   * @throws IllegalArgumentException If the point has a different number of dimensions
   */
  public MBR expand(double[] point) {
    if (point.length != dimensions) {
      throw new IllegalArgumentException("Point dimensions don't match MBR dimensions");
    }

    double[] newMins = mins.clone();
    double[] newMaxs = maxs.clone();

    for (int i = 0; i < dimensions; i++) {
      if (isEmpty() || point[i] < newMins[i]) {
        newMins[i] = point[i];
      }
      if (isEmpty() || point[i] > newMaxs[i]) {
        newMaxs[i] = point[i];
      }
    }

    return new MBR(newMins, newMaxs);
  }

  /**
   * Creates a new MBR that is the union of this MBR and another MBR.
   *
   * @param other The other MBR to unite with
   * @return A new MBR that contains both this MBR and the other MBR
   * @throws IllegalArgumentException If the other MBR has a different number of dimensions
   */
  public MBR union(MBR other) {
    if (other.dimensions != dimensions) {
      throw new IllegalArgumentException("MBR dimensions don't match");
    }

    if (isEmpty()) {
      return new MBR(other);
    }

    if (other.isEmpty()) {
      return new MBR(this);
    }

    double[] newMins = new double[dimensions];
    double[] newMaxs = new double[dimensions];

    for (int i = 0; i < dimensions; i++) {
      newMins[i] = Math.min(mins[i], other.mins[i]);
      newMaxs[i] = Math.max(maxs[i], other.maxs[i]);
    }

    return new MBR(newMins, newMaxs);
  }

  /**
   * Calculates the enlargement needed to include the given MBR.
   * This is the difference between the area of the union and the area of this MBR.
   *
   * @param other The MBR to be included
   * @return The area increase required to include the other MBR
   * @throws IllegalArgumentException If the other MBR has a different number of dimensions
   */
  public double calculateEnlargement(MBR other) {
    if (other.dimensions != dimensions) {
      throw new IllegalArgumentException("MBR dimensions don't match");
    }

    if (isEmpty()) {
      return other.area();
    }

    if (other.isEmpty()) {
      return 0;
    }

    MBR union = union(other);
    return union.area() - area();
  }

  /**
   * Calculates the n-dimensional volume (area in 2D, volume in 3D, etc.) of this MBR.
   *
   * @return The n-dimensional volume of this MBR
   */
  public double area() {
    if (isEmpty()) {
      return 0;
    }

    double area = 1.0;
    for (int i = 0; i < dimensions; i++) {
      double extent = getExtent(i);
      if (Double.isInfinite(extent)) {
        return Double.POSITIVE_INFINITY;
      }
      area *= extent;
    }

    return area;
  }

  /**
   * Calculates the sum of all edge lengths (perimeter in 2D, surface area in 3D, etc.).
   *
   * @return The sum of all edge lengths
   */
  public double perimeter() {
    if (isEmpty()) {
      return 0;
    }

    double perimeter = 0.0;
    for (int i = 0; i < dimensions; i++) {
      double extent = getExtent(i);
      if (Double.isInfinite(extent)) {
        return Double.POSITIVE_INFINITY;
      }
      perimeter += extent;
    }

    return perimeter * Math.pow(2, dimensions - 1);
  }

  /**
   * Returns the extent (width, height, etc.) of this MBR in the specified dimension.
   *
   * @param dimension The dimension index (0-based)
   * @return The extent in the specified dimension
   * @throws IndexOutOfBoundsException If the dimension is invalid
   */
  public double getExtent(int dimension) {
    if (dimension < 0 || dimension >= dimensions) {
      throw new IndexOutOfBoundsException("Invalid dimension: " + dimension);
    }

    if (isEmpty()) {
      return 0;
    }

    return maxs[dimension] - mins[dimension];
  }

  /**
   * Determines if this MBR overlaps with another MBR.
   *
   * @param other The other MBR
   * @return true if the MBRs overlap, false otherwise
   * @throws IllegalArgumentException If the other MBR has a different number of dimensions
   */
  public boolean overlaps(MBR other) {
    if (other.dimensions != dimensions) {
      throw new IllegalArgumentException("MBR dimensions don't match");
    }

    if (isEmpty() || other.isEmpty()) {
      return false;
    }

    for (int i = 0; i < dimensions; i++) {
      if (maxs[i] < other.mins[i] || mins[i] > other.maxs[i]) {
        return false;
      }
    }

    return true;
  }

  /**
   * Calculates the amount of overlap between this MBR and another MBR.
   *
   * @param other The other MBR
   * @return The n-dimensional volume of the overlap, or 0 if they don't overlap
   * @throws IllegalArgumentException If the other MBR has a different number of dimensions
   */
  public double overlapAmount(MBR other) {
    if (other.dimensions != dimensions) {
      throw new IllegalArgumentException("MBR dimensions don't match");
    }

    if (isEmpty() || other.isEmpty() || !overlaps(other)) {
      return 0;
    }

    double overlap = 1.0;
    for (int i = 0; i < dimensions; i++) {
      double min = Math.max(mins[i], other.mins[i]);
      double max = Math.min(maxs[i], other.maxs[i]);
      double extent = max - min;

      if (Double.isInfinite(extent)) {
        // Handle infinite overlap in a dimension
        if (Double.isInfinite(mins[i]) && mins[i] == other.mins[i] &&
            Double.isInfinite(maxs[i]) && maxs[i] == other.maxs[i]) {
          continue; // Same infinite extent, contributes factor of 1
        } else {
          extent = 1.0; // Different infinite extents, use normalized factor
        }
      }

      overlap *= extent;
    }

    return overlap;
  }

  /**
   * Creates a new MBR that is the intersection of this MBR and another MBR.
   *
   * @param other The other MBR
   * @return A new MBR that is the intersection, or an empty MBR if they don't intersect
   * @throws IllegalArgumentException If the other MBR has a different number of dimensions
   */
  public MBR intersection(MBR other) {
    if (other.dimensions != dimensions) {
      throw new IllegalArgumentException("MBR dimensions don't match");
    }

    if (isEmpty() || other.isEmpty() || !overlaps(other)) {
      return new MBR(dimensions); // Empty MBR
    }

    double[] newMins = new double[dimensions];
    double[] newMaxs = new double[dimensions];

    for (int i = 0; i < dimensions; i++) {
      newMins[i] = Math.max(mins[i], other.mins[i]);
      newMaxs[i] = Math.min(maxs[i], other.maxs[i]);
    }

    return new MBR(newMins, newMaxs);
  }

  /**
   * Checks if this MBR contains the specified point.
   *
   * @param point The point to check
   * @return true if the point is contained in this MBR, false otherwise
   * @throws IllegalArgumentException If the point has a different number of dimensions
   */
  public boolean contains(double[] point) {
    if (point.length != dimensions) {
      throw new IllegalArgumentException("Point dimensions don't match MBR dimensions");
    }

    if (isEmpty()) {
      return false;
    }

    for (int i = 0; i < dimensions; i++) {
      if (point[i] < mins[i] || point[i] > maxs[i]) {
        return false;
      }
    }

    return true;
  }

  /**
   * Checks if this MBR contains another MBR.
   *
   * @param other The MBR to check
   * @return true if the other MBR is fully contained in this MBR, false otherwise
   * @throws IllegalArgumentException If the other MBR has a different number of dimensions
   */
  public boolean contains(MBR other) {
    if (other.dimensions != dimensions) {
      throw new IllegalArgumentException("MBR dimensions don't match");
    }

    if (isEmpty() || other.isEmpty()) {
      return false;
    }

    for (int i = 0; i < dimensions; i++) {
      if (other.mins[i] < mins[i] || other.maxs[i] > maxs[i]) {
        return false;
      }
    }

    return true;
  }

  /**
   * Calculates the minimum distance between this MBR and a point.
   *
   * @param point The point
   * @return The minimum distance between the MBR and the point
   * @throws IllegalArgumentException If the point has a different number of dimensions
   */
  public double minDistance(double[] point) {
    if (point.length != dimensions) {
      throw new IllegalArgumentException("Point dimensions don't match MBR dimensions");
    }

    if (isEmpty()) {
      return Double.POSITIVE_INFINITY;
    }

    if (contains(point)) {
      return 0.0;
    }

    double sumSquared = 0.0;
    for (int i = 0; i < dimensions; i++) {
      if (point[i] < mins[i]) {
        double dist = mins[i] - point[i];
        sumSquared += dist * dist;
      } else if (point[i] > maxs[i]) {
        double dist = point[i] - maxs[i];
        sumSquared += dist * dist;
      }
      // If the point's coordinate is within the MBR's range in this dimension,
      // it contributes 0 to the distance
    }

    return Math.sqrt(sumSquared);
  }

  /**
   * Calculates the minimum distance between this MBR and another MBR.
   *
   * @param other The other MBR
   * @return The minimum distance between the two MBRs
   * @throws IllegalArgumentException If the other MBR has a different number of dimensions
   */
  public double minDistance(MBR other) {
    if (other.dimensions != dimensions) {
      throw new IllegalArgumentException("MBR dimensions don't match");
    }

    if (isEmpty() || other.isEmpty()) {
      return Double.POSITIVE_INFINITY;
    }

    if (overlaps(other)) {
      return 0.0;
    }

    double sumSquared = 0.0;
    for (int i = 0; i < dimensions; i++) {
      if (maxs[i] < other.mins[i]) {
        double dist = other.mins[i] - maxs[i];
        sumSquared += dist * dist;
      } else if (mins[i] > other.maxs[i]) {
        double dist = mins[i] - other.maxs[i];
        sumSquared += dist * dist;
      }
      // If the MBRs overlap in this dimension, it contributes 0 to the distance
    }

    return Math.sqrt(sumSquared);
  }

  /**
   * Returns the center point of this MBR.
   *
   * @return An array representing the center point coordinates
   */
  public double[] getCenter() {
    if (isEmpty()) {
      return null;
    }

    double[] center = new double[dimensions];
    for (int i = 0; i < dimensions; i++) {
      if (Double.isInfinite(mins[i]) && Double.isInfinite(maxs[i])) {
        center[i] = 0.0; // Arbitrary center for infinite dimension
      } else if (Double.isInfinite(mins[i])) {
        center[i] = maxs[i] - 1.0; // Arbitrary point before max
      } else if (Double.isInfinite(maxs[i])) {
        center[i] = mins[i] + 1.0; // Arbitrary point after min
      } else {
        center[i] = (mins[i] + maxs[i]) / 2.0; // Regular center
      }
    }

    return center;
  }

  @Override
  public String toString() {
    if (isEmpty()) {
      return "MBR[empty, dimensions=" + dimensions + "]";
    }

    StringBuilder sb = new StringBuilder();
    sb.append("MBR[dimensions=").append(dimensions).append(", ");

    for (int i = 0; i < dimensions; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append("dim").append(i).append("=(");

      if (mins[i] == Double.NEGATIVE_INFINITY) {
        sb.append("-∞");
      } else {
        sb.append(mins[i]);
      }

      sb.append(", ");

      if (maxs[i] == Double.POSITIVE_INFINITY) {
        sb.append("+∞");
      } else {
        sb.append(maxs[i]);
      }

      sb.append(")");
    }

    sb.append("]");
    return sb.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    MBR other = (MBR) obj;
    if (dimensions != other.dimensions) {
      return false;
    }

    // Two empty MBRs are equal regardless of their min/max values
    if (isEmpty() && other.isEmpty()) {
      return true;
    }

    for (int i = 0; i < dimensions; i++) {
      if (Double.compare(mins[i], other.mins[i]) != 0 ||
          Double.compare(maxs[i], other.maxs[i]) != 0) {
        return false;
      }
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hash = 7;

    if (!isEmpty()) {
      for (int i = 0; i < dimensions; i++) {
        hash = 31 * hash + Double.hashCode(mins[i]);
        hash = 31 * hash + Double.hashCode(maxs[i]);
      }
    } else {
      hash = 31 * hash + dimensions;
    }

    return hash;
  }
}
