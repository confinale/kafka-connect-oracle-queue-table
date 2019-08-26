package ch.confinale.kafka.connect.oracle.queue.table.source.services

import org.apache.kafka.common.errors.RetriableException

internal class RecoverableOracleException(cause: Throwable) : RetriableException(cause)
