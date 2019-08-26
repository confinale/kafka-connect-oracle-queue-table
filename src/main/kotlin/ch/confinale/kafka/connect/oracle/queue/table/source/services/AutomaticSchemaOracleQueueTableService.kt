package ch.confinale.kafka.connect.oracle.queue.table.source.services

import org.apache.kafka.connect.data.Date
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Timestamp
import java.sql.Connection
import java.sql.ResultSetMetaData
import java.sql.Types

class AutomaticSchemaOracleQueueTableService(
        private val connection: Connection,
        private val tableName: String,
        topicName: String,
        maxRows: Long
) : OracleQueueTableService(connection, tableName, topicName, maxRows) {

    private var schm: Schema? = null

    override fun getSchema(): Schema {
        if (schm != null) {
            return schm as Schema
        }
        val metaData = connection.prepareStatement("SELECT * FROM $tableName WHERE ROWNUM = 0").metaData
        val schemaBuilder = SchemaBuilder.struct()
        for (columnNr in 1..metaData.columnCount) {
            val columnName = metaData.getColumnName(columnNr)
            val columnType = metaData.getColumnType(columnNr)
            val nullable = metaData.isNullable(columnNr)
            val fieldSchema = getFieldSchema(columnType, nullable)
                    ?: throw RuntimeException("Unable to resolve schema for column $columnName")
            schemaBuilder.field(columnName, fieldSchema)
        }
        return schemaBuilder.build()
    }

    private fun getFieldSchema(columnType: Int, nullable: Int): Schema? {
        when (nullable) {
            ResultSetMetaData.columnNullable -> {
                when (columnType) {
                    Types.INTEGER -> return Schema.OPTIONAL_INT32_SCHEMA
                    Types.VARCHAR -> return Schema.OPTIONAL_STRING_SCHEMA
                    Types.BOOLEAN -> return Schema.OPTIONAL_BOOLEAN_SCHEMA
                    Types.TIMESTAMP -> return Timestamp.builder().optional().build()
                    Types.DATE -> return Date.builder().optional().build()
                }
            }
            ResultSetMetaData.columnNoNulls -> {
                when (columnType) {
                    Types.INTEGER -> return Schema.INT32_SCHEMA
                    Types.VARCHAR -> return Schema.STRING_SCHEMA
                    Types.BOOLEAN -> return Schema.BOOLEAN_SCHEMA
                    Types.TIMESTAMP -> return Timestamp.SCHEMA
                    Types.DATE -> return Date.SCHEMA
                }
            }
        }
        return null
    }

}
