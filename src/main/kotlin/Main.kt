import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.JUnitCore
import kotlin.time.Duration
import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import kotlin.time.DurationUnit
import kotlin.time.toDuration

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
    val duration: Duration
)

class MetricsService {

    private val logEntriesPerMethodAndResource = mutableMapOf<String, MutableList<LogEntry>>()

    fun consumeLogEntry(logEntry: String) {
        val entryElement = logEntry.split(" ")
        val method = entryElement[1].substring(1)
        val resource = entryElement[2]

        val newEntry = LogEntry(
            parseDateTime(entryElement[0].trim('[', ']')),
            method,
            resource,
            entryElement[4].toInt(),
            entryElement[5].toInt().toDuration(DurationUnit.MILLISECONDS)
        )

        logEntriesPerMethodAndResource.getOrPut(method + resource) { mutableListOf() }.add(newEntry)
    }

    fun getAggregatedMetrics(method: String, resource: String, limit: Int): List<AggregatedMetrics> {
        val now = LocalDateTime.now()

        val firstReq =
        val totalReqs
        for (entry in logEntriesPerMethod[method + resource]) {
            if (entry.timestamp.minus(now) > limit) continue

            if entry.timestamp < firstReq

            totalReqs++
        }

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
