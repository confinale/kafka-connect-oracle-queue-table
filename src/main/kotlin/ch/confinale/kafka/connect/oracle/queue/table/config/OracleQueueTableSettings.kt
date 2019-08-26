package ch.confinale.kafka.connect.oracle.queue.table.config

import ch.confinale.kafka.connect.oracle.queue.table.config.OracleQueueTableConfigConstants.Companion.KCQL_REGEX
import java.util.*

class OracleQueueTableSettings(config: OracleQueueTableConfig) {

    val pollingTimeout = config.getLong(OracleQueueTableConfigConstants.POLLING_TIMEOUT_CONFIG) ?: 0L
    private val jdbcUrl = config.getString(OracleQueueTableConfigConstants.CONNECTION_STRING)!!
    private val username = config.getString(OracleQueueTableConfigConstants.USERNAME)!!
    private val password = config.getPassword(OracleQueueTableConfigConstants.PASSWORD)!!
    val maxRows = config.getLong(OracleQueueTableConfigConstants.MAX_ROWS)!!

    val datasourceProperties = Properties()
            .also { it.setProperty("jdbcUrl", jdbcUrl) }
            .also { it.setProperty("username", username) }
            .also { it.setProperty("password", password.value()) }

    val subTasks = config.getString(OracleQueueTableConfigConstants.KCQL).split(";")
            .asSequence()
            .map { it.trim() }
            .filter { it.isNotBlank() }
            .map { Regex(KCQL_REGEX).find(it) }
            .map { it ?: throw IllegalStateException("Unable to parse KCQL: $it") }
            .map { it.destructured }
            .map { (topic, table, schema) ->
                SubTask(topic, table, schema)
            }
            .toList()

    data class SubTask(val topic: String, val table: String, val schema: String)
}
