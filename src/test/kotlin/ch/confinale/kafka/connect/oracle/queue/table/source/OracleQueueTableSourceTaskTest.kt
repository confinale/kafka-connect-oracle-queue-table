package ch.confinale.kafka.connect.oracle.queue.table.source

import io.mockk.*
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTaskContext
import org.junit.Assert.assertEquals
import org.junit.Test
import java.sql.*
import java.util.*

class OracleQueueTableSourceTaskTest {

    @Test
    fun poll() {
        val oracleQueueTableSourceTask = OracleQueueTableSourceTask()
        val connectionUrl = "jdbc:oracle:thin:@hostname:1522:sid"
        val username = "user"
        val password = "pass"
        val schemaUrl = OracleQueueTableSourceTaskTest::class.java.getResource("OracleQueueTableSourceTaskTest.avro")
        val props = mutableMapOf(
                Pair("connect.oracle.queue.table.connection.string", connectionUrl),
                Pair("connect.oracle.queue.table.kcql", "INSERT INTO topicName FROM tableName WITH SCHEMA $schemaUrl"),
                Pair("connect.oracle.queue.table.max.rows", "200"),
                Pair("connect.oracle.queue.table.username", username),
                Pair("connect.oracle.queue.table.password", password)
        )
        val sourceTaskContext = mockk<SourceTaskContext>()
        every { sourceTaskContext.configs() } returns (mutableMapOf())
        oracleQueueTableSourceTask.initialize(sourceTaskContext)
        oracleQueueTableSourceTask.start(props)
        val driver = mockk<Driver>()
        every { driver.acceptsURL(connectionUrl) } returns true
        val connection = mockk<Connection>()
        every { driver.connect(connectionUrl, Properties().also { it.setProperty("user", username) }.also { it.setProperty("password", password) }) } returns connection
        every { connection.isReadOnly } returns false
        every { connection.autoCommit } returns true
        every { connection.isValid(1) } returns true
        every { connection.transactionIsolation } returns Connection.TRANSACTION_NONE
        every { connection.networkTimeout } returns 1000
        every { connection.setNetworkTimeout(any(), any()) } just Runs
        every { connection.isValid(5) } returns true
        every { connection.autoCommit = any() } just Runs
        every { connection.clearWarnings() } just Runs
        DriverManager.registerDriver(driver)

        val preparedStatement = mockk<PreparedStatement>()
        every { connection.prepareStatement(any()) } returns preparedStatement
        every { preparedStatement.maxRows = 200 } just Runs
        every { preparedStatement.execute() } returns true
        val resultSet = mockk<ResultSet>()
        every { preparedStatement.resultSet } returns resultSet
        every { resultSet.next() } returns false
        every { connection.commit() } just Runs

        val expected = mutableListOf<SourceRecord>()
        val actual = oracleQueueTableSourceTask.poll()

        verify(exactly = 1) { connection.commit() }

        assertEquals(expected, actual)
    }
}
