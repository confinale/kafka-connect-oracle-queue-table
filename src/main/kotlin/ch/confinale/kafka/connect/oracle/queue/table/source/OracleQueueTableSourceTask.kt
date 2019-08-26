package ch.confinale.kafka.connect.oracle.queue.table.source

import ch.confinale.kafka.connect.oracle.queue.table.config.OracleQueueTableConfig
import ch.confinale.kafka.connect.oracle.queue.table.config.OracleQueueTableSettings
import ch.confinale.kafka.connect.oracle.queue.table.source.services.OracleQueueTableService
import ch.confinale.kafka.connect.oracle.queue.table.source.services.OracleQueueTableServicePool
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask
import org.slf4j.LoggerFactory
import java.util.*

class OracleQueueTableSourceTask : SourceTask() {

    companion object {
        @Suppress("JAVA_CLASS_ON_COMPANION")
        private val log = LoggerFactory.getLogger(javaClass.enclosingClass)
    }

    private val rowIdsToDelete = emptyMap<String, String>().toMutableMap()
    private val services = emptyMap<String, OracleQueueTableService>().toMutableMap()
    private lateinit var settings: OracleQueueTableSettings
    private var running = false

    override fun stop() {
        log.trace("[${Thread.currentThread().id}] stop")
        running = false
    }

    override fun version(): String {
        val version = "1"
        log.trace("[${Thread.currentThread().id}] version=$version")
        return version
    }


    override fun poll(): MutableList<SourceRecord> {
        log.trace("[${Thread.currentThread().id}] poll")
        var allSourceRecords = emptyList<Pair<SourceRecord, Pair<String, String>>>().toMutableList()
        for (subTask in settings.subTasks) {
            val pollEventUuid = uniqueUUID(rowIdsToDelete.values)
            log.trace("[${Thread.currentThread().id}] poll eventUuid=$pollEventUuid table=${subTask.table} topic=${subTask.topic}")
            val pool = OracleQueueTableServicePool.getPool(settings)
            val service = pool.getService(subTask.table, subTask.topic, subTask.schema, settings.maxRows)
            val tableSourceRecords = service.select()
            log.trace("[${Thread.currentThread().id}] poll eventUuid=$pollEventUuid records=${tableSourceRecords.size}")
            if (tableSourceRecords.isEmpty()) {
                service.commitAndClose()
            } else {
                services[pollEventUuid] = service
                allSourceRecords.addAll(tableSourceRecords.map {
                    Pair(
                            it,
                            Pair(it.sourceOffset()["rowId"].toString(), pollEventUuid)
                    )
                })
            }
            rowIdsToDelete.putAll(tableSourceRecords.map { Pair(it.sourceOffset()["rowId"].toString(), pollEventUuid) })
        }
        if (allSourceRecords.isEmpty() && settings.pollingTimeout > 0) {
            log.trace("[${Thread.currentThread().id}] poll sleep timeout=${settings.pollingTimeout}")
            Thread.sleep(settings.pollingTimeout)
        }
        log.trace("[${Thread.currentThread().id}] poll sourceRecords=${allSourceRecords.size}")
        return allSourceRecords.map { it.first }.toMutableList()
    }

    override fun start(props: MutableMap<String, String>) {
        running = true
        settings =
                OracleQueueTableSettings(OracleQueueTableConfig(if (context.configs().isEmpty()) props else context.configs()))
        for (subTask in settings.subTasks) {
            log.debug("[${Thread.currentThread().id}] start subtask schema=${subTask.schema} table=${subTask.table}")
        }
    }

    override fun commitRecord(record: SourceRecord) {
        super.commitRecord(record)
        val table = record.sourcePartition()!!["table"].toString()
        val rowId = record.sourceOffset()!!["rowId"].toString()
        val pollEventUuid = rowIdsToDelete[rowId]
        val service = services[pollEventUuid]!!
        log.trace("[${Thread.currentThread().id}] commitRecord table=$table eventUuid=$pollEventUuid rowId=$rowId")
        try {
            service.delete(rowId)
        } finally {
            rowIdsToDelete.remove(rowId)
        }
        if (!rowIdsToDelete.values.contains(pollEventUuid)) {
            service.commitAndClose()
            services.remove(pollEventUuid)
            log.trace("[${Thread.currentThread().id}] commit eventUuid=$pollEventUuid")
            if (!running && services.isEmpty()) {
                OracleQueueTableServicePool.getPool(settings).close()
            }
        }
    }

    private fun uniqueUUID(collisions: Collection<String>): String {
        return UUID.randomUUID().toString().let { if (collisions.contains(it)) uniqueUUID(collisions) else it }
    }
}
