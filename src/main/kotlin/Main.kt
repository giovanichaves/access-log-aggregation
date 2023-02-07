import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.JUnitCore
import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

val TEST_DATA_FILE = {}::class.java.getResource("access.log")?.toURI()
val DATETIME_FORMAT = DateTimeFormatter.ISO_DATE_TIME

fun readTestData() = File(TEST_DATA_FILE).readLines().asSequence()
fun parseDateTime(isoDateTime: String) = LocalDateTime.parse(isoDateTime, DATETIME_FORMAT)

data class AggregatedMetrics(
    val aggregatedMinute: LocalDateTime,
    val firstRequest: LocalDateTime?,
    val lastRequest: LocalDateTime?,
    val numberOfRequests: Int,
    val averageTimeToServeInMs: Int?,
    val maximumTimeToServeInMs: Int?,
    val minimumTimeToServeInMs: Int?
)

data class LogEntry(
    val timestamp: LocalDateTime,
    val method: String,
    val resource: String,
    val statusCode: Int,
    val duration: Int
)

class MetricsService {

    private val logEntriesPerMethodAndResource = mutableMapOf<String, MutableMap<LocalDateTime, MutableList<LogEntry>>>()

    fun consumeLogEntry(logEntry: String) {
        val entryElement = logEntry.split(" ")
        val method = entryElement[1].substring(1)
        val resource = entryElement[2]

        val newEntry = LogEntry(
            parseDateTime(entryElement[0].trim('[', ']')),
            method,
            resource,
            entryElement[4].toInt(),
            entryElement[5].toInt()
        )

        logEntriesPerMethodAndResource.getOrPut(method + resource) { mutableMapOf() }
            .getOrPut(newEntry.timestamp.truncatedTo(ChronoUnit.MINUTES)) { mutableListOf() }
                .add(newEntry)
    }

    fun getAggregatedMetrics(method: String, resource: String, limit: Int): List<AggregatedMetrics> {
        if (limit < 1) return emptyList()

        val methodResourceEntries = logEntriesPerMethodAndResource[method + resource] ?: return emptyList()

        val lastXMinutesEntries = methodResourceEntries.lastXMinutes(limit)

        return lastXMinutesEntries.entries.map { it.value.aggregateEntries(it.key) }
    }

    private fun Map<LocalDateTime, MutableList<LogEntry>>.lastXMinutes(limit: Int) = this.toSortedMap(compareByDescending { it }).asSequence().take(limit).map { it.toPair() }.toMap()

    private fun List<LogEntry>.aggregateEntries(minute: LocalDateTime): AggregatedMetrics {
        if (this.isEmpty()) return AggregatedMetrics(minute, null, null, 0, null, null, null)

        var firstReq: LocalDateTime = this.first().timestamp
        var lastReq: LocalDateTime = this.first().timestamp
        var totalDuration = 0
        var minimumTimeToServeInMs = Int.MAX_VALUE
        var maximumTimeToServeInMs = 0
        for (entry in this) {
            if (entry.timestamp.isBefore(firstReq)) firstReq = entry.timestamp
            if (entry.timestamp.isAfter(lastReq)) lastReq = entry.timestamp
            totalDuration += entry.duration
            if (entry.duration < minimumTimeToServeInMs) minimumTimeToServeInMs = entry.duration
            if (entry.duration > maximumTimeToServeInMs) maximumTimeToServeInMs = entry.duration
        }

        return AggregatedMetrics(
            minute,
            firstReq,
            lastReq,
            this.size,
            totalDuration / this.size,
            minimumTimeToServeInMs,
            maximumTimeToServeInMs
        )
    }
}

class UnitTests {
    @Test
    fun testCaseExampleUse() {
        val metrics = MetricsService().apply { readTestData().forEach(this::consumeLogEntry) }
        val results = metrics.getAggregatedMetrics(
            "GET",
            "/quotes/latest",
            5
        )
        assertEquals(5, results.size)
    }
}

fun main() {
    JUnitCore.main(UnitTests::class.java.canonicalName)
}
