# Open Range Representation in MBRs

## Context

Our multi-dimensional Minimal Bounding Rectangle (MBR) implementation
needs to support open/unbounded ranges. We must decide how to represent
these unbounded extents in our data structure. The options include:

1. Using `Double.MAX_VALUE` and `-Double.MAX_VALUE` as bounds
2. Using `Double.POSITIVE_INFINITY` and `Double.NEGATIVE_INFINITY`
3. Using sentinel values (e.g., specific large constants)
4. Using separate boolean flags to indicate unbounded dimensions

This decision impacts numerical stability, interoperability with other
libraries, and the semantics of geometric operations.

## Decision

We will use `Double.POSITIVE_INFINITY` and `Double.NEGATIVE_INFINITY` to
represent unbounded ranges in our MBR implementation.

## Rationale

Using infinity values provides several significant advantages:

- **Mathematical Correctness**: Infinity values properly represent the
  concept of unbounded ranges in a mathematically rigorous way.

- **Arithmetic Propagation**: Infinity values propagate correctly
  through arithmetic operations. For example, `POSITIVE_INFINITY + 5`
  remains `POSITIVE_INFINITY`, whereas `Double.MAX_VALUE + 5` might
  overflow.

- **Semantic Clarity**: Infinity directly expresses the intent that a
  range is unbounded, rather than "very large but finite."

- **Library Alignment**: Major spatial libraries like JTS Topology
  Suite, GeoTools, and Spatial4j all use infinity values, improving
  interoperability.

- **Numerical Stability**: Operations involving infinity are
  well-defined in IEEE 754 floating-point arithmetic, avoiding potential
  numerical errors.

- **Distinction from Empty**: Using infinity allows clear
  differentiation between empty/null MBRs and infinite ones, which have
  different semantic meanings in spatial operations.

- **Dimension Consistency**: The approach works consistently across all
  dimensions in our n-dimensional implementation.

- **Operation Simplification**: Intersection, union, and containment
  checks are simpler to implement with infinity values.

## Implementation

Our implementation will:

1. Use `Double.NEGATIVE_INFINITY` for minimum bounds of unbounded dimensions.
2. Use `Double.POSITIVE_INFINITY` for maximum bounds of unbounded dimensions.
3. Initialize "empty" or "null" MBRs with min =
   `Double.POSITIVE_INFINITY` and max = `Double.NEGATIVE_INFINITY` (an
   invalid state that indicates emptiness).
4. Provide helper methods to check if an MBR is empty, infinite, or
   half-infinite.

## Consequences

### Positive
- Clear semantic representation of unbounded ranges
- Improved interoperability with major spatial libraries
- Mathematically correct behavior in computations
- Simplified implementation of geometric operations
- Consistent behavior across dimensions
- Better handling of edge cases

### Negative
- Potential issues when serializing/deserializing (infinity values may
  need special handling)
- May require special handling in UI representations
- Some performance overhead in checking for infinity values
- Difficulty in visualizing infinite ranges in debug scenarios

## Alternatives Considered

### Double.MAX_VALUE / -Double.MAX_VALUE
Rejected because:
- Not mathematically correct for representing infinity
- Potential for overflow in arithmetic operations
- Less clear semantically than actual infinity values
- Inconsistent with major spatial libraries

### Sentinel Values
Rejected because:
- Arbitrary and less semantically clear
- Requires extra documentation and institutional knowledge
- Would need special handling in all arithmetic operations
- Does not leverage IEEE 754 behavior for infinity

### Boolean Flags
Rejected because:
- Complicates the data structure and API
- Requires special handling in all operations
- Less efficient in computations
- Not aligned with standard approach in spatial libraries
