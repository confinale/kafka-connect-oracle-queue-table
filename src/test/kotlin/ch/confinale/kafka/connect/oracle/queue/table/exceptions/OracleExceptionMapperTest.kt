package ch.confinale.kafka.connect.oracle.queue.table.exceptions

import ch.confinale.kafka.connect.oracle.queue.table.source.services.RecoverableOracleException
import org.junit.Test
import java.net.SocketException
import java.sql.SQLRecoverableException
import kotlin.test.assertTrue

class OracleExceptionMapperTest {


    @Test
    fun sqlException() {
        val ex = SQLRecoverableException("IO Error: Connection reset")
        ex.initCause(SocketException("Connection reset"))
        assertTrue(OracleExceptionMapper.getKnownException(ex) is RecoverableOracleException)
    }
}
