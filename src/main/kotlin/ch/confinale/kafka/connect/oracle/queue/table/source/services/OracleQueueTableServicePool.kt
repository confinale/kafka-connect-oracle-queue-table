package ch.confinale.kafka.connect.oracle.queue.table.source.services

import ch.confinale.kafka.connect.oracle.queue.table.config.OracleQueueTableSettings
import ch.confinale.kafka.connect.oracle.queue.table.exceptions.OracleExceptionMapper
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.apache.kafka.common.errors.DisconnectException
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import javax.sql.DataSource

class OracleQueueTableServicePool private constructor(
        private val dataSource: DataSource
) {

    companion object {

        @Suppress("JAVA_CLASS_ON_COMPANION")
        private val log = LoggerFactory.getLogger(javaClass.enclosingClass)

        fun getPool(settings: OracleQueueTableSettings): OracleQueueTableServicePool {
            val dataSource = getDataSource(settings.datasourceProperties)
            return OracleQueueTableServicePool(dataSource)
        }

        private var dataSources = ConcurrentHashMap<Pair<Thread, Properties>, DataSource>()

        private fun getDataSource(properties: Properties): DataSource {
            val key = Pair(Thread.currentThread(), properties)
            return dataSources.computeIfAbsent(key) { createDataSource(properties) }
        }

        private fun createDataSource(properties: Properties): DataSource {
            val hikariConfig = HikariConfig(properties)
            hikariConfig.maximumPoolSize = 1
            hikariConfig.poolName = "thread-${Thread.currentThread().id}"
            try {
                return HikariDataSource(hikariConfig)
            } catch (ex: Exception) {
                OracleExceptionMapper.getKnownException(ex)?.also { throw it }
                throw ex
            }
        }
    }


    fun getService(tableName: String, topicName: String, schema: String, maxRows: Long): OracleQueueTableService {
        return ManualSchemaOracleQueueTableService(getConnection(), tableName, topicName, schema, maxRows)
    }

    fun getService(tableName: String, topicName: String, maxRows: Long): OracleQueueTableService {
        return AutomaticSchemaOracleQueueTableService(getConnection(), tableName, topicName, maxRows)
    }

    fun close() {
        dataSource.unwrap(HikariDataSource::class.java).close()
        (dataSources.keys.firstOrNull { it.first == Thread.currentThread() })?.also { dataSources.remove(it) }
        log.trace("[${Thread.currentThread().id}] Datasource closed")
    }

    private fun getConnection(): Connection {
        val connection = try {
            dataSource.connection
        } catch (e: Exception) {
            close()
            throw DisconnectException(e)
        }
        log.trace("[${Thread.currentThread().id}] Created connection $connection")
        return connection.also { it.autoCommit = false }
    }

}
