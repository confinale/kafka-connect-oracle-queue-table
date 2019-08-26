package ch.confinale.kafka.connect.oracle.queue.table.exceptions

import ch.confinale.kafka.connect.oracle.queue.table.source.services.RecoverableOracleException
import java.sql.SQLException
import java.sql.SQLRecoverableException
import java.util.function.Function
import java.util.function.Predicate

class OracleExceptionMapper {

    companion object {

        private val KNOWN_EXCEPTIONS: Map<Predicate<Throwable>, Function<Throwable, Exception>> = mapOf(
                Pair(Predicate { it is SQLRecoverableException }, Function { RecoverableOracleException(it) }),
                Pair(Predicate { it is SQLException && it.message!!.contains("ORA-01035") }, Function { RecoverableOracleException(it) })
        )

        fun getKnownException(throwable: Throwable): Exception? {
            return getKnownException(throwable, throwable)
        }

        private fun getKnownException(throwable: Throwable, main: Throwable): Exception? {
            var t = KNOWN_EXCEPTIONS.entries.firstOrNull { it.key.test(throwable) }?.value?.apply(main)
            t = if (t == null && throwable.cause != null) getKnownException(throwable.cause!!, throwable) else t
            return t
        }

    }
}
