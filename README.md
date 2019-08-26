Kafka Connect Oracle Queue Table 
================================
[![CircleCI](https://circleci.com/gh/confinale/kafka-connect-oracle-queue-table.svg?style=svg)](https://circleci.com/gh/confinale/kafka-connect-oracle-queue-table)

This source connector was developed as alternative to JMS connector using Advanced Queuing of Oracle Database. It uses features of Oracle Database on which Advanced Queuing is built on but simplifies things by only requiring standard database table which is representing queue. Rows are polled from the queue table using Oracle specific query _SELECT ... FOR UPDATE SKIP LOCKED_. This locks the rows for parallel reading until _COMMIT_ is performed and allows reading of distinct row in parallel by several connections. Because the rows are deleted (using _ROWID_) as soon as the data are successfully written to Kafka topic, the feature prevents processing same rows twice. 

### Advantages
* Processing in batches (using JDBC feature of _maxRows_)
* Simplicity - only table required on database side
* No processing of data on the database - enqueuing just by _INSERT_ statement
* No complex types or serialization of data

### Disadvantages
* Table allows only flat structure of data
* Batch processing can theoretically produce more not deleted rows on connection drop
* Queue might not be handled by connector in order in sequence of inserting
* No advanced features like prioritization, timestamping or exception tracking

### Configuration
| Name                                         | Description                           | Type   | Default | Valid values                              |
| ---------------------------------------------|---------------------------------------|--------|---------|-------------------------------------------|
| connect.oracle.queue.table.connection.string | Oracle JDBC connection string         | string | 0       |                                           | 
| connect.oracle.queue.table.polling.timeout   | Timeout between pollings with no data | long   |         |                                           | 
| connect.oracle.queue.table.username          | Database connection username          | string |         |                                           |
| connect.oracle.queue.table.password          | Database connection password          | string |         |                                           |
| connect.oracle.queue.table.kcql              | KCQL string                           | string |         |                                           |
| connect.oracle.queue.table.max.rows          | Maximum number of rows polled         | long   |         | lower than maximum number of open cursors |

## KCQL

Connector uses KCQL (inspired by connectors by Landoop) for configuring from which table to which Kafka topic data should be polled. Valid KCQL string must follow this pattern  _INSERT INTO topicName FROM tableName WITH SCHEMA urlOfSchema_. Semicolon as separator between KCQL configurations allows iterating over polling from multiple tables in one connector task.
