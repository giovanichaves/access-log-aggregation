We want you to build the business logic of a service that consumes a web server access log and calculates important live metrics.

This service:

1. processes new entries as they appear in the access log. The access log contains lines with the following format (already filtered to contain only successful GET requests):
> [\<TIMESTAMP\>] "<METHOD> <RESOURCE> HTTP/1.1" 200 <DURATION_IN_MS>

2. exposes aggregated metrics data per method+resource for each minute containing the following information:
   * first request timestamp
   * last request timestamp
   * number of requests
   * average time to serve request
   * maximum time to serve request
   * minimum time to serve request

A file with an example access log is provided.

# API:
The two functions in the class MetricsService for consuming and serving have the following signatures respectively:
>fun consumeLogEntry(logEntry: String): Unit

>fun getAggregatedMetrics(method: String, resource: String, limit: Int): List<AggregatedMetrics>

Implement MetricsService (and anything you need to support it) as suitable to solve the task. Use in-memory data structures to hold the data. AggregatedMetrics should be used as provided in the boilerplate code.