package ch.confinale.kafka.connect.oracle.queue.table.source.services

import io.confluent.connect.avro.AvroData
import io.mockk.*
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import org.junit.Assert.assertEquals
import org.junit.Test
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.*

class ManualSchemaOracleQueueTableServiceTest {

    @Test
    fun select() {
        val now = LocalDateTime.now()

        val connection = mockk<Connection>()
        val preparedStatement = mockk<PreparedStatement>()
        val resultSet = mockk<ResultSet>()

        val tableName = "tableName"
        val topic = "topicName"

        val schemaUrl = ManualSchemaOracleQueueTableServiceTest::class.java.getResource("testSchema.avro")
        val maxRows = 200L
        val oracleQueueTableService = ManualSchemaOracleQueueTableService(connection, tableName, topic, schemaUrl.toString(), maxRows)
        val valueSchema = AvroData(8).toConnectSchema(org.apache.avro.Schema.Parser().parse(schemaUrl.openStream()))
        val expecteValue = Struct(valueSchema)
                .also { it.put("id", 1L) }
                .also { it.put("active", true) }
                .also { it.put("textEnglish", "Switzerland") }
                .also { it.put("lastModifiedAt", Timestamp.valueOf(now)) }
        val expected = arrayListOf(SourceRecord(
                Collections.singletonMap("table", tableName),
                Collections.singletonMap("rowId", "abcdefgh"),
                topic,
                Schema.STRING_SCHEMA,
                "$tableName.abcdefgh",
                valueSchema,
                expecteValue
        ))


        every { connection.prepareStatement("SELECT ROWID, t.* FROM $tableName t FOR UPDATE SKIP LOCKED") }.returns(preparedStatement)
        every { preparedStatement.maxRows = maxRows.toInt() } just Runs
        every { preparedStatement.execute() } returns true
        every { preparedStatement.resultSet } returns resultSet
        every { resultSet.next() } returns true andThen false
        every { resultSet.getString(1) } returns "abcdefgh"
        every { resultSet.getLong("ID") } returns 1L
        every { resultSet.getString("ACTIVE") } returns "+"
        every { resultSet.getString("TEXT_ENGLISH") } returns "Switzerland"
        every { resultSet.getTimestamp("LAST_MODIFIED_AT") } returns Timestamp.valueOf(now)
        every { resultSet.wasNull() } returns false

        val actual = oracleQueueTableService.select()

        verify(exactly = 0) { connection.commit() }
        verify(exactly = 1) { connection.prepareStatement(any()) }
        verify(exactly = 1) { preparedStatement.maxRows = 200 }
        verify(exactly = 1) { preparedStatement.execute() }
        verify(exactly = 1) { preparedStatement.resultSet }
        verify(exactly = 1) { preparedStatement.close() }
        verify(exactly = 2) { resultSet.next() }
        verify(exactly = 1) { resultSet.getString(1) } // ROWID column
        verify { resultSet.getString(any<String>()) }
        verify { resultSet.getLong(any<String>()) }
        verify { resultSet.wasNull() }
        verify { resultSet.getTimestamp(any<String>()) }
        verify(exactly = 1) { resultSet.close() }

        confirmVerified(connection)
        confirmVerified(preparedStatement)
        confirmVerified(resultSet)


        assertEquals(expected, actual)
    }
}
