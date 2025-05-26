# Rama Java Coding Standard

This coding standard aims to provide a consistent approach to writing
Java code for Rama applications.

## 1. Package Structure

- Use lowercase package names
- Group packages by feature or module functionality
- Primary package prefix is `com.rpl.rama.helpers`

## 2. Naming Conventions

### Classes
- Use PascalCase (e.g., `GraphModule`, `RecommendationModule`)
- Module classes should be suffixed with "Module" (e.g.,
  `CollaborativeDocumentEditorModule`)
- Use descriptive, meaningful names that reflect the purpose of the class

### Methods
- Use camelCase (e.g., `addNode`, `getNeighbors`)
- Names should clearly express what the method does
- Ensure method names are descriptive enough to understand their purpose
  without looking at the implementation

### Variables
- Use camelCase for variable names
- Avoid single-character names except in loops or lambda expressions
- Choose descriptive names that indicate purpose and usage
- Depot variables should be prefixed with an asterisk (e.g., `*graphDepot`)

### Constants
- Use UPPER_SNAKE_CASE (e.g., `MAX_SIZE`, `DEFAULT_CAPACITY`)
- Declare as `static final`

## 3. Code Formatting

### Indentation
- Use 2 spaces for indentation (not tabs)
- Maintain consistent indentation throughout the codebase

### Braces
- Opening braces go on the same line as the statement (K&R style)
- Closing braces go on their own line unless followed by `else`, `catch`, or similar
- Always use braces for control structures, even for single-line blocks

### Line Length
- Aim for a maximum line length of 100 characters
- Split long lines at logical points for readability
- For method chaining, place each method call on a new line with
  consistent indentation

### Whitespace
- Use a single blank line to separate logical sections within methods
- Use two blank lines to separate class definitions
- Use a single space after keywords like `if`, `for`, `while`, etc.
- Use a single space around operators (assignment, arithmetic, etc.)

## 4. Documentation

### Class Documentation
- Each class should have a JavaDoc comment describing its purpose and
  functionality
- Include author and version information when appropriate

### Method Documentation
- All public methods should have JavaDoc comments
- Describe parameters with `@param` tags
- Document return values with `@return` tags
- Document exceptions with `@throws` tags
- Include examples for complex APIs

### Code Comments
- Use comments to explain "why" rather than "what"
- Keep comments concise and focused
- Update comments when code changes

## 5. Rama-Specific Conventions

### Module Structure
- Rama modules should be organized with depots at the top, followed by
  implementation details
- Clearly separate processing logic from view definitions
- Each module should have a clearly defined responsibility

### Depots
- Depot variable names should be prefixed with an asterisk (e.g., `*graphDepot`)
- Use the module name or functionality as a prefix for depots to prevent
  naming conflicts

### ETL Topologies
- Keep ETL topologies focused on a single responsibility
- Use descriptive names for the ETL topologies that indicate their purpose
- Use method references instead of lambda expressions when possible

### Views and Indexes
- Use clear, descriptive names for PState variables
- Name PState variables based on what they store rather than how they're used
- Organize views by their logical relationship to data

## 6. Error Handling

### Exceptions
- Use specific exception types rather than generic ones
- Document exceptions in method JavaDocs
- Handle exceptions at the appropriate level
- Don't catch exceptions you can't handle properly

### Validation
- Validate inputs at the entry points of public methods
- Use appropriate error messages that clearly indicate the issue
- Consider using preconditions for parameter validation

## 7. Testing

### Test Organization
- Tests should mirror the package structure of the code they're testing
- Use descriptive test method names that indicate the scenario being tested
- Follow the "Arrange-Act-Assert" pattern in test methods

### Test Coverage
- Aim for comprehensive test coverage of all public APIs
- Include both positive and negative test cases
- Test edge cases and error conditions

## 8. Performance Considerations

### Memory Usage
- Be mindful of memory usage in data structures
- Prefer primitive types over boxed primitives when appropriate
- Consider the memory implications of large collections

### Rama-Specific Optimizations
- Choose appropriate partitioning strategies for depots based on access patterns
- Design data models to minimize cross-partition operations
- Be aware of serialization costs when passing data between partitions

## 9. Libraries and Dependencies

### Standard Libraries
- Use Java standard library classes when available
- Avoid reinventing functionality that exists in the standard library

### Rama Libraries
- Leverage Rama's built-in functionality rather than reimplementing it
- Follow Rama's recommended patterns for common tasks

## 10. Code Examples

### Module Declaration
```java
public class GraphModule implements RamaModule {
  // Depot for graph operations
  public static final String *GRAPH_DEPOT = "*graphDepot";

  @Override
  public void define(Setup setup, Topologies topologies) {
    // Define depot
    setup.declareDepot(
      *GRAPH_DEPOT,
      new IntegerPartitioner());

    // Define ETL topology
    ETLTopology etl = topologies.etlTopology("graph-processor");

    // Implementation
    // ...
  }
}
```

### Method Style
```java
/**
 * Adds a directed edge between two nodes in the graph.
 *
 * @param source The source node ID
 * @param target The target node ID
 * @return True if the edge was added, false if it already existed
 */
public boolean addEdge(int source, int target) {
  if (source == target) {
    return false;  // Self-loops not allowed
  }

  return edges.putIfAbsent(new Edge(source, target), true) == null;
}
```

### Lambda Expression Style
```java
// Preferred style for short lambdas
List<Integer> evenNumbers = numbers.stream()
  .filter(n -> n % 2 == 0)
  .collect(Collectors.toList());

// Style for multi-line lambdas
List<String> formattedResults = results.stream()
  .map(result -> {
    String formatted = format(result);
    return normalized ? normalized(formatted) : formatted;
  })
  .collect(Collectors.toList());
```
