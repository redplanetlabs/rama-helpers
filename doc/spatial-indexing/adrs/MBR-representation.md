# Minimal Bounding Rectangle Representation

## Context

We require efficient manipulation of Minimal Bounding Rectangles (MBRs)
for geographic and spatial data processing across multiple dimensions
(>2D). We need to select between two common representation approaches:

1. Min/Max coordinates
   (min[0], max[0], min[1], max[1], ..., min[n], max[n])
2. Position/Size coordinates
   (position[0], position[1], ..., position[n], size[0], size[1], ..., size[n])

This decision impacts performance, code readability, compatibility with
external libraries, and scalability across dimensions.

## Decision

We will use the Min/Max coordinate representation for n-dimensional MBRs.

## Rationale

For multi-dimensional geographic and spatial workloads, the Min/Max
representation offers several advantages:

- **Geometric Operations**: Facilitates more efficient intersection,
  containment, and overlap calculations, which are common in spatial
  queries

- **Geographic Compatibility**: More natural for geographic coordinate
  systems (latitude/longitude) and extends cleanly to higher dimensions

- **Merging Efficiency**: Simpler algorithm for combining multiple MBRs
  (take min of mins, max of maxes) regardless of dimensionality

- **Coordinate Queries**: Easier extraction of specific points (e.g.,
  corners) and boundary values

- **Library Compatibility**: Better alignment with common geospatial
  libraries (JTS, GeoTools, Spatial4j) and multi-dimensional extensions

- **Numerical Stability**: Generally more robust against floating-point
  precision issues in geometric calculations

- **Transformation Clarity**: Clearer semantics when transforming
  between coordinate systems

- **Dimensional Scalability**: Extends naturally to n-dimensions through
  arrays or vectors of min/max pairs

- **Consistent Operations**: Query patterns remain consistent regardless
  of dimensionality

While Position/Size offers some advantages for UI rendering, our
primarily geographic and spatial workload makes Min/Max the more
appropriate choice, especially as dimensions increase beyond 2D.

## Consequences

### Positive
- Improved performance for spatial query operations in any number of dimensions
- Better integration with major geospatial libraries
- More intuitive code for geographic data processing across dimensions
- Reduced complexity for common spatial operations
- Consistent algorithm patterns regardless of dimensionality
- Flexible support for variable dimension counts
- Better scalability as dimension requirements change
- Simpler implementation of hypercube (n-cube) operations

### Negative
- Slightly more conversion work when interfacing with UI components
- Potential slight increase in code complexity for certain scaling operations
- May require adapters for graphics libraries that use Position/Size representation
- Less optimized for the special case of exactly 2D operations
- Increased memory usage for very high dimensions (though this affects any representation)
- Potentially more difficult to debug due to generic implementation

## Alternatives Considered
Position/Size representation was considered but rejected due to:
1. The predominantly spatial nature of our workload
2. The advantages of Min/Max for multi-dimensional operations
3. Increased complexity of Position/Size representation in higher dimensions
4. Poor scaling of Position/Size to high-dimensional spaces where corner
   calculations become more complex
