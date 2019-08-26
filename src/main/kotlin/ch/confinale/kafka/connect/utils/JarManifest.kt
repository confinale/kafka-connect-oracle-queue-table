package ch.confinale.kafka.connect.utils

import java.net.URL
import java.util.jar.JarInputStream

class JarManifest(location: URL) {

    val map = emptyMap<String, String?>().toMutableMap()

    init {
        val jarFile = JarInputStream(location.openStream())
        if (jarFile.manifest != null) {
            val manifest = jarFile.manifest
            val attributes = manifest.mainAttributes
            map.putAll(listOf(
                    "Built-By",
                    "Build-Timestamp",
                    "Git-Commit-Hash",
                    "Git-Tag",
                    "Version",
                    "Created-By",
                    "Build-Jdk",
                    "Build-OS"
            ).map { Pair(it, attributes.getValue(it)?.toString()) })
        }
    }

    fun version(): String {
        return map["Version"] ?: "unknown"
    }

    fun buildTimestamp(): String {
        return map["Build-Timestamp"] ?: "unknown"
    }

    fun printManifest(): String {
        return map.map { "|${"${it.key}:".padEnd(29)}${it.value ?: "unknown"}" }
                .joinToString("\n")
    }

}
