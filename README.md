## Concurrent Cache Testing

This project introduces a comprehensive testing framework designed to evaluate the performance of different Least Recently Used (LRU) cache implementations integrated with a simulated database environment. 

The framework tests under various configurations and scenarios to benchmark the efficiency, reliability, and scalability of cache strategies within concurrent systems.

A sample configuration of an LRU cache is available. It includes features such as request collapsing and various threading models to simulate different real-world usage scenarios.

### How It Works
The testing framework operates by executing the following steps:

1. **Generate Requests**: Different patterns of requests are generated to simulate varied user interaction patterns with the cache.
2. **Organize Requests**: Requests are organized according to specified strategies to test the cache's response to different access patterns.
3. **Simulate Cache Operation**: Each cache configuration is tested with the organized requests. The cache interacts with the simulated database, handling GET and SET operations.
4. **Measure Performance**: After executing the tests, the system reports various metrics which detail the effectiveness and efficiency of the cache configuration under test.

### What You Get Out of It

1. Understand **performance impacts** of different cache configurations.
2. Diagnose potential scalability and **reliability issues** in cache implementations.

### How to Run

Just run the main program in *CacheTester*.
