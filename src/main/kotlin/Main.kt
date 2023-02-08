
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.JUnitCore
import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import kotlin.math.log2

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

    private val logEntriesPerMethodAndResource = mutableMapOf<String, LinkedHashMap<LocalDateTime, MutableList<LogEntry>>>()

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

        logEntriesPerMethodAndResource.getOrPut(method + resource) { linkedMapOf() }
            .getOrPut(newEntry.timestamp.truncatedTo(ChronoUnit.MINUTES)) { mutableListOf() }
                .add(newEntry)
    }

    fun getAggregatedMetrics(method: String, resource: String, limit: Int): List<AggregatedMetrics> {
        if (limit < 1) return emptyList()

        val methodResourceEntries = logEntriesPerMethodAndResource[method + resource] ?: return emptyList()

        val lastXMinutesEntries = methodResourceEntries.lastXMinutes(limit)

        return lastXMinutesEntries.entries.map { it.value.aggregateEntries(it.key) }
    }

    fun LinkedHashMap<LocalDateTime, MutableList<LogEntry>>.lastXMinutes(limit: Int): LinkedHashMap<LocalDateTime, MutableList<LogEntry>> {
        if (this.isEmpty()) return this

        if (limit < log2(this.values.size.toDouble())) {
            //O(NL)
            return lastXMinutesScanSearch(limit)
        }

        //O(N logN)
        return this.toSortedMap(compareByDescending { it }).asSequence().take(limit).map { it.toPair() }.toMap() as LinkedHashMap
    }

    fun LinkedHashMap<LocalDateTime, MutableList<LogEntry>>.lastXMinutesScanSearch(limit: Int): LinkedHashMap<LocalDateTime, MutableList<LogEntry>> {
        val lastXMinutes = linkedMapOf<LocalDateTime, MutableList<LogEntry>>()
        var latestMinute: LocalDateTime? = null
        var lastInserted: LocalDateTime? = null
        repeat(limit) {
            this.forEach {
                if ((latestMinute == null || it.key > latestMinute) && (lastInserted == null || it.key < lastInserted)) {
                    latestMinute = it.key
                }
            }
            lastXMinutes[latestMinute!!] = this[latestMinute] as MutableList<LogEntry>
            lastInserted = latestMinute
            latestMinute = null
        }
        return lastXMinutes
    }

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

    @Test
    fun `returns last X minutes items ordered`() {
        val l1630 = LocalDateTime.parse("2023-02-02T16:30:00") to mutableListOf(
            LogEntry(LocalDateTime.parse("2023-02-02T16:30:35"), "GET", "/test", 200, 80),
            LogEntry(LocalDateTime.parse("2023-02-02T16:30:32"), "GET", "/test", 200, 100),
            LogEntry(LocalDateTime.parse("2023-02-02T16:30:10"), "GET", "/test", 200, 40)
        )
        val l1622 = LocalDateTime.parse("2023-02-02T16:22:00") to mutableListOf(
            LogEntry(LocalDateTime.parse("2023-02-02T16:22:05"), "GET", "/test", 200, 80),
            LogEntry(LocalDateTime.parse("2023-02-02T16:22:22"), "GET", "/test", 200, 100),
            LogEntry(LocalDateTime.parse("2023-02-02T16:22:10"), "GET", "/test", 200, 40)
        )
        val l1613 = LocalDateTime.parse("2023-02-02T16:13:00") to mutableListOf(
            LogEntry(LocalDateTime.parse("2023-02-02T16:13:15"), "GET", "/test", 200, 80),
            LogEntry(LocalDateTime.parse("2023-02-02T16:13:22"), "GET", "/test", 200, 100),
            LogEntry(LocalDateTime.parse("2023-02-02T16:13:10"), "GET", "/test", 200, 40)
        )
        val l1611 = LocalDateTime.parse("2023-02-02T16:11:00") to mutableListOf(
            LogEntry(LocalDateTime.parse("2023-02-02T16:11:05"), "GET", "/test", 200, 80),
            LogEntry(LocalDateTime.parse("2023-02-02T16:11:22"), "GET", "/test", 200, 100),
            LogEntry(LocalDateTime.parse("2023-02-02T16:11:10"), "GET", "/test", 200, 40)
        )
        val l1610 = LocalDateTime.parse("2023-02-02T16:10:00") to mutableListOf(
            LogEntry(LocalDateTime.parse("2023-02-02T16:10:25"), "GET", "/test", 200, 80),
            LogEntry(LocalDateTime.parse("2023-02-02T16:10:22"), "GET", "/test", 200, 100),
            LogEntry(LocalDateTime.parse("2023-02-02T16:10:50"), "GET", "/test", 200, 40)
        )
        val l1604 = LocalDateTime.parse("2023-02-02T16:04:00") to mutableListOf(
            LogEntry(LocalDateTime.parse("2023-02-02T16:04:25"), "GET", "/test", 200, 80),
            LogEntry(LocalDateTime.parse("2023-02-02T16:04:42"), "GET", "/test", 200, 100),
            LogEntry(LocalDateTime.parse("2023-02-02T16:04:10"), "GET", "/test", 200, 40)
        )
        val l1600 = LocalDateTime.parse("2023-02-02T16:00:00") to mutableListOf(
            LogEntry(LocalDateTime.parse("2023-02-02T16:00:25"), "GET", "/test", 200, 80),
            LogEntry(LocalDateTime.parse("2023-02-02T16:00:12"), "GET", "/test", 200, 100),
            LogEntry(LocalDateTime.parse("2023-02-02T16:00:10"), "GET", "/test", 200, 40)
        )
        val logEntries = linkedMapOf(
            l1604,
            l1611,
            l1600,
            l1630,
            l1613,
            l1622,
            l1610,
            )

            val result = with(MetricsService()) { logEntries.lastXMinutesScanSearch(3) }

            assertEquals(mapOf(l1630, l1622, l1613), result)
    }
}

fun main() {
    JUnitCore.main(UnitTests::class.java.canonicalName)
}
