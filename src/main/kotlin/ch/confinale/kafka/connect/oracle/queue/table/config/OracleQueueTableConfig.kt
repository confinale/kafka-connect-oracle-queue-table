package ch.confinale.kafka.connect.oracle.queue.table.config

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.slf4j.LoggerFactory

class OracleQueueTableConfig(props: Map<String, String>) :
        AbstractConfig(CONFIG, props, log.isInfoEnabled) {

    companion object : ConfigDef() {
        @Suppress("JAVA_CLASS_ON_COMPANION")
        private val log = LoggerFactory.getLogger(javaClass.enclosingClass)

        val CONFIG = ConfigDef()
                .define(
                        OracleQueueTableConfigConstants.POLLING_TIMEOUT_CONFIG, Type.LONG, 0L, Importance.LOW,
                        OracleQueueTableConfigConstants.POLLING_TIMEOUT_CONFIG_DOC, "Connection", 1,
                        Width.MEDIUM, OracleQueueTableConfigConstants.POLLING_TIMEOUT_CONFIG
                )
                .define(
                        OracleQueueTableConfigConstants.CONNECTION_STRING, Type.STRING, Importance.HIGH,
                        OracleQueueTableConfigConstants.CONNECTION_STRING_DOC, "Connection", 2,
                        Width.MEDIUM, OracleQueueTableConfigConstants.CONNECTION_STRING
                )
                .define(
                        OracleQueueTableConfigConstants.USERNAME, Type.STRING, Importance.HIGH,
                        OracleQueueTableConfigConstants.USERNAME_DOC, "Connection", 3,
                        Width.MEDIUM, OracleQueueTableConfigConstants.USERNAME
                )
                .define(
                        OracleQueueTableConfigConstants.PASSWORD, Type.PASSWORD, Importance.HIGH,
                        OracleQueueTableConfigConstants.PASSWORD_DOC, "Connection", 4,
                        Width.MEDIUM, OracleQueueTableConfigConstants.PASSWORD
                )
                .define(
                        OracleQueueTableConfigConstants.KCQL, Type.STRING, Importance.HIGH,
                        OracleQueueTableConfigConstants.KCQL_DOC, "Connection", 5,
                        Width.MEDIUM, OracleQueueTableConfigConstants.KCQL
                )
                .define(
                        OracleQueueTableConfigConstants.MAX_ROWS, Type.LONG, Importance.HIGH,
                        OracleQueueTableConfigConstants.MAX_ROWS_DOC, "Connection", 6,
                        Width.MEDIUM, OracleQueueTableConfigConstants.MAX_ROWS
                )!!

    }
}
