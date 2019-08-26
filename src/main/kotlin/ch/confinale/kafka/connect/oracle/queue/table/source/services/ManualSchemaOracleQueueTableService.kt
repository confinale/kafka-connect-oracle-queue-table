package ch.confinale.kafka.connect.oracle.queue.table.source.services

import io.confluent.connect.avro.AvroData
import org.apache.kafka.connect.data.Schema
import java.net.URL
import java.sql.Connection

class ManualSchemaOracleQueueTableService(
        connection: Connection,
        tableName: String,
        kafkaTopic: String,
        private val schema: String,
        maxRows: Long
) : OracleQueueTableService(connection, tableName, kafkaTopic, maxRows) {

    companion object {
        private val avroData = AvroData(8)
    }

    private var connectSchema: Schema? = null

    override fun getSchema(): Schema {
        if (connectSchema == null) {
            val avroSchema = org.apache.avro.Schema.Parser().parse(
                    URL(schema).openStream()
            )
            connectSchema = avroData.toConnectSchema(avroSchema)
        }
        return connectSchema!!
    }

}
