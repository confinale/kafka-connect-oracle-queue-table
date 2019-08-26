package ch.confinale.kafka.connect.oracle.queue.table.source

import ch.confinale.kafka.connect.oracle.queue.table.config.OracleQueueTableConfigConstants
import org.junit.Assert.assertEquals
import org.junit.Test

class OracleQueueTableSourceConnectorTest {

    @Test
    fun taskConfigs() {
        val connector = OracleQueueTableSourceConnector()
        var input = mapOf(Pair(OracleQueueTableConfigConstants.KCQL, "INSERT INTO test FROM test WITH SCHEMA classpath:static/avro/testSchema.avsc"))
        connector.start(input)
        val taskConfigs = connector.taskConfigs(5)
        assertEquals(5, taskConfigs!!.size)
        assertEquals("INSERT INTO test FROM test WITH SCHEMA classpath:static/avro/testSchema.avsc", taskConfigs[0][OracleQueueTableConfigConstants.KCQL])
        assertEquals("INSERT INTO test FROM test WITH SCHEMA classpath:static/avro/testSchema.avsc", taskConfigs[1][OracleQueueTableConfigConstants.KCQL])
    }
}
