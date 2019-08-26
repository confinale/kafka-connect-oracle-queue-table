package ch.confinale.kafka.connect.oracle.queue.table.source.services

import ch.confinale.kafka.connect.oracle.queue.table.exceptions.OracleExceptionMapper
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.Timestamp
import org.apache.kafka.connect.source.SourceRecord
import org.slf4j.LoggerFactory
import java.sql.*
import java.util.*
import java.util.Date


abstract class OracleQueueTableService(
        private val connection: Connection,
        private val tableName: String,
        private val kafkaTopic: String,
        private val maxRows: Long
) {

    companion object {
        @Suppress("JAVA_CLASS_ON_COMPANION")
        private val log = LoggerFactory.getLogger(javaClass.enclosingClass)
    }


    fun commitAndClose() {
        synchronized(this) {
            try {
                if (!connection.isClosed) {
                    log.trace("[${Thread.currentThread().id}] Commiting connection $connection")
                    connection.commit()
                    log.trace("[${Thread.currentThread().id}]Closing connection $connection")
                    connection.close()
                }
            } catch (ex: Exception) {
                OracleExceptionMapper.getKnownException(ex)?.also { throw it }
                throw ex
            }
        }
    }

    fun select(): List<SourceRecord> {
        val connectSchema = getSchema()
        val output = listOf<SourceRecord>().toMutableList()
        var statement: PreparedStatement? = null
        try {
            val selectQuery = "SELECT ROWID, t.* FROM $tableName t FOR UPDATE SKIP LOCKED"
            statement = connection.prepareStatement(selectQuery)
            statement.maxRows = maxRows.toInt()

            val resultSet: ResultSet?
            try {
                try {
                    val execute = statement.execute()
                    if (!execute) {
                        return output
                    }
                    resultSet = statement.resultSet
                } catch (ex: Exception) {
                    OracleExceptionMapper.getKnownException(ex)?.also { throw it }
                    throw ex
                }
            } catch (ex: SQLSyntaxErrorException) {
                throw SQLSyntaxErrorException("Error executing query $selectQuery", ex)
            }
            try {
                while (resultSet.next()) {
                    val rowId = resultSet.getString(1)
                    val struct = Struct(connectSchema)
                    connectSchema.fields().forEach {
                        val columnName = toDBCaseFormat(it.name())
                        try {
                            when {
                                it.schema().name() == org.apache.kafka.connect.data.Decimal.LOGICAL_NAME ->
                                    struct.put(
                                            it,
                                            getResulSetValue(resultSet, columnName, resultSet::getBigDecimal)?.let { v ->
                                                v.setScale(
                                                        it.schema().parameters()[org.apache.kafka.connect.data.Decimal.SCALE_FIELD]
                                                                .let { s -> Integer.parseInt(s) }
                                                )
                                            }
                                    )
                                it.schema().name() == org.apache.kafka.connect.data.Date.LOGICAL_NAME ->
                                    struct.put(
                                            it,
                                            getResulSetValue(resultSet, columnName, resultSet::getDate)?.let { v -> toUTC(v) }
                                    )
                                it.schema().name() == Timestamp.LOGICAL_NAME ->
                                    struct.put(
                                            it,
                                            getResulSetValue(resultSet, columnName, resultSet::getTimestamp)
                                    )
                                matchesType(it.schema(), Schema.Type.FLOAT32) ->
                                    struct.put(it, getResulSetValue(resultSet, columnName, resultSet::getFloat))
                                matchesType(it.schema(), Schema.Type.FLOAT64) ->
                                    struct.put(it, getResulSetValue(resultSet, columnName, resultSet::getDouble))
                                matchesType(it.schema(), Schema.Type.BYTES) ->
                                    struct.put(it, getResulSetValue(resultSet, columnName, resultSet::getBytes))
                                matchesType(it.schema(), Schema.Type.BOOLEAN) ->
                                    struct.put(it, getResulSetValue(resultSet, columnName, resultSet::getString)?.let { v -> "+" == v })
                                matchesType(it.schema(), Schema.Type.INT32) ->
                                    struct.put(it, getResulSetValue(resultSet, columnName, resultSet::getInt))
                                matchesType(it.schema(), Schema.Type.INT64) ->
                                    struct.put(it, getResulSetValue(resultSet, columnName, resultSet::getLong))
                                matchesType(it.schema(), Schema.Type.STRING) ->
                                    struct.put(it, getResulSetValue(resultSet, columnName, resultSet::getString))
                                else -> throw RuntimeException("Unsupported schema")
                            }
                        } catch (ex: Exception) {
                            commitAndClose()
                            throw Exception("Error reading column $columnName", ex)
                        }
                    }

                    val keysValue = "$tableName.$rowId"
                    val sourceRecord = SourceRecord(
                            Collections.singletonMap("table", tableName),
                            Collections.singletonMap("rowId", rowId),
                            kafkaTopic,
                            Schema.STRING_SCHEMA,
                            keysValue,
                            connectSchema,
                            struct
                    )
                    output.add(sourceRecord)
                }
            } finally {
                try {
                    resultSet.close()
                } catch (ignore: Exception) {
                }
            }
        } finally {
            try {
                statement?.close()
            } catch (ignore: Exception) {
            }

        }
        return output
    }

    private fun <T> getResulSetValue(resultSet: ResultSet, columneName: String, resultSetFn: (columnName: String) -> T): T? {
        val invoke = resultSetFn.invoke(columneName)
        if (resultSet.wasNull()) {
            return null
        }
        return invoke
    }

    private fun toUTC(sqlDate: java.sql.Date): Date {
        val localDate = sqlDate.toLocalDate()
        val utcCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
        utcCalendar.set(Calendar.YEAR, localDate.year)
        utcCalendar.set(Calendar.MONTH, localDate.monthValue - 1)
        utcCalendar.set(Calendar.DAY_OF_MONTH, localDate.dayOfMonth)
        listOf(Calendar.HOUR_OF_DAY, Calendar.MINUTE, Calendar.SECOND, Calendar.MILLISECOND).forEach { utcCalendar.set(it, 0) }
        return utcCalendar.time
    }

    abstract fun getSchema(): Schema

    private fun matchesType(schema: Schema, matchedType: Schema.Type): Boolean {
        return schema.type() == matchedType
    }

    private fun matchesType(schema: org.apache.avro.Schema, matchedType: org.apache.avro.Schema.Type): Boolean {
        return schema.type == matchedType ||
                (schema.type == org.apache.avro.Schema.Type.UNION && schema.types.map { it.type }.let {
                    it.size == 2 && it.containsAll(
                            listOf(matchedType, org.apache.avro.Schema.Type.NULL)
                    )
                })
    }

    private fun toDBCaseFormat(it: String): String {
        return it.replace(Regex("([A-Z])"), "_$1").toUpperCase()
    }

    fun delete(rowId: String) {
        val deleteStatement = "DELETE FROM $tableName WHERE ROWID = '$rowId'"
        try {
            try {
                connection.prepareStatement(deleteStatement).execute()
            } catch (ex: SQLException) {
                commitAndClose()
                throw SQLException("Error executing query $deleteStatement", ex)
            }
        } catch (ex: Exception) {
            OracleExceptionMapper.getKnownException(ex)?.also { throw it }
            throw ex
        }
    }

}


