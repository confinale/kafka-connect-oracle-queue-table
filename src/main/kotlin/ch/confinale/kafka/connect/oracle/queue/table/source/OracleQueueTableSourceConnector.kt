package ch.confinale.kafka.connect.oracle.queue.table.source

import ch.confinale.kafka.connect.oracle.queue.table.config.OracleQueueTableConfig
import ch.confinale.kafka.connect.oracle.queue.table.config.OracleQueueTableConfigConstants
import ch.confinale.kafka.connect.oracle.queue.table.config.OracleQueueTableConfigConstants.Companion.KCQL_REGEX
import ch.confinale.kafka.connect.utils.JarManifest
import org.apache.kafka.common.config.Config
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector
import org.apache.kafka.connect.util.ConnectorUtils
import org.slf4j.LoggerFactory
import kotlin.math.min

class OracleQueueTableSourceConnector : SourceConnector() {

    companion object {
        @Suppress("JAVA_CLASS_ON_COMPANION")
        private val log = LoggerFactory.getLogger(javaClass.enclosingClass)
    }

    private var connectorName = "non-started"
    private lateinit var configProps: Map<String, String>
    private val configDef = OracleQueueTableConfig.CONFIG
    private val manifest = JarManifest(javaClass.protectionDomain.codeSource.location)

    override fun start(props: Map<String, String>) {
        connectorName = props.getOrDefault("name", "unknown")
        configProps = props
        log.trace("[$connectorName] start")
    }

    override fun taskClass(): Class<out Task> {
        return OracleQueueTableSourceTask::class.java
    }

    override fun taskConfigs(maxTasks: Int): List<Map<String, String>>? {
        val subTasks = configProps.getValue(OracleQueueTableConfigConstants.KCQL).split(";")
                .flatMap { (0 until maxTasks).map { _ -> it } }
        log.trace("[$connectorName] taskConfigs maxTasks=$maxTasks subTasks=${subTasks.size}")
        return ConnectorUtils.groupPartitions(subTasks, min(subTasks.size, maxTasks))
                .map { g ->
                    configProps.toMutableMap().also { it[OracleQueueTableConfigConstants.KCQL] = g.joinToString(";") }
                }
    }

    override fun stop() {
        log.trace("[$connectorName] stop")
    }

    override fun config(): ConfigDef? {
        return configDef
    }

    override fun version(): String? {
        val version = manifest.version()
        val buildTimestamp = manifest.buildTimestamp()
        log.trace("[$connectorName] version=$version")
        return "$version build at $buildTimestamp"
    }

    override fun validate(connectorConfigs: MutableMap<String, String>?): Config {
        log.trace("[$connectorName] validate")
        val config = super.validate(connectorConfigs)
        config.configValues().filter { it.name() == OracleQueueTableConfigConstants.KCQL }
                .map { it.value().toString() }
                .map { kcql ->
                    kcql.split(";")
                            .map {
                                if (!Regex(KCQL_REGEX).matches(it)) {
                                    throw KcqlException("$it is invalid KCQL")
                                }
                            }
                }
        return config
    }

    class KcqlException(message: String) : RuntimeException(message)
}

