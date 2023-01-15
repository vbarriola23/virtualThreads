# virtualThreads

## intent:
In this project, I am using virtual threads to showcase different strategies you can follow when you request units of work that require both I/O tasks and resource-expensive CPU-intensive calculations.

## BatchProcess:
- a real life simplified example of a mission-critical application that monitors different machinery in power plants and quickly determines if they are functioning correctly
  - we assume that the machines have sensors connected to the network that provides reading on temperature, pressure, etc.
  - we have to connect to these devices, get their data readings, and quickly do a computationally intense data analysis to report back on the system's state
- entries are continously read and placed into a list of type InputEntries created by a record on line 33
- we then fetch and process the data
  - in this simple example, we only fetch data from one sensor, but in a more realistic application, we might need to process several sensors with multiple I/O operations on each virtual thread.
  - the virtual threads are fetching the data and running in parallel
- analyzing the data
  - assume that the data analysis we have to perform is CPU intensive
  - ideally, we would like to perform this calculation in the same thread to maintain the thread per unit of work paradigm. But, for a CPU-bound workload, it is best to use thread pools to get optimal performance for time sensitive analysis
  - this code uses Java Steams, which under the covers, uses ForkJoinPool for its calculations
  - semaphores are used to throttle the utilization of a scarce resource by the virtual threads

## BatchProcessesPools
- same idea as BatchProcess, but utilizes explicitly threadpools instead of using parallel java Streams
- employs fixedThreadPool and chachedTreadPool
- doesn't use the ForkJoinPool as it is implicitly used in the BatchProcess class.
