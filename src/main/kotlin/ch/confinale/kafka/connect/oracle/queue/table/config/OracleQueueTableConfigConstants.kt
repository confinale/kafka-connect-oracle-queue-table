package ch.confinale.kafka.connect.oracle.queue.table.config

class OracleQueueTableConfigConstants {
    companion object {
        private const val CONNECTOR_PREFIX = "connect.oracle.queue.table"
        const val CONNECTION_STRING = "$CONNECTOR_PREFIX.connection.string"
        const val CONNECTION_STRING_DOC = "Oracle JDBC connection string"
        const val POLLING_TIMEOUT_CONFIG = "$CONNECTOR_PREFIX.polling.timeout"
        const val POLLING_TIMEOUT_CONFIG_DOC = "Timeout between pollings with no data"
        const val USERNAME = "$CONNECTOR_PREFIX.username"
        const val USERNAME_DOC = "Database connection username"
        const val PASSWORD = "$CONNECTOR_PREFIX.password"
        const val PASSWORD_DOC = "Database connection password"
        const val KCQL = "$CONNECTOR_PREFIX.kcql"
        const val KCQL_DOC = "KCQL expression describing field selection and routes."
        const val KCQL_REGEX = "INSERT\\s+INTO\\s+(\\S+)\\s+FROM\\s+(\\S+)\\s+WITH\\s+SCHEMA\\s+(\\S+)"
        const val MAX_ROWS = "$CONNECTOR_PREFIX.max.rows"
        const val MAX_ROWS_DOC = "Maximum number of rows returned by SELECT FOR UPDATE query (must be lower than maximum number of open cursors)"
    }

}
