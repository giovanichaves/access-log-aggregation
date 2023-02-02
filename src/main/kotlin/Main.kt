import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.runner.JUnitCore
import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

val TEST_DATA_FILE = "/home/coderpad/data/access.log"
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

class MetricsService {

    fun consumeLogEntry(logEntry: String) {
        TODO()
    }

    fun getAggregatedMetrics(method: String, resource: String, limit: Int): List<AggregatedMetrics> {
        TODO()
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
