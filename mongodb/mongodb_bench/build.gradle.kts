plugins {
    kotlin("jvm") version "1.9.10" // Adjusted to a stable Kotlin version
}

group = "org.example"
version = "1.0-SNAPSHOT"
var ktor_version = "2.3.4" // Use consistent Ktor version

repositories {
    mavenCentral()
    maven {
        url = uri("https://repo.osgeo.org/repository/release")
    }
}

dependencies {
    testImplementation(kotlin("test"))

    // MongoDB
    implementation("org.mongodb:mongodb-driver-kotlin-coroutine:5.2.1")
    implementation("org.mongodb:mongodb-driver-sync:5.2.1")
    implementation("org.mongodb:bson:5.2.1")
    implementation("org.mongodb:bson-kotlinx:5.2.1")

    // Geospatial
    implementation("org.locationtech.jts:jts-core:1.18.2")
    implementation("org.locationtech.proj4j:proj4j:1.1.0")

    // CSV Parsing
    implementation("com.opencsv:opencsv:5.5.2")

    // Kotlinx Coroutines
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")

    // Ktor Server
    implementation("io.ktor:ktor-server-core:$ktor_version")
    implementation("io.ktor:ktor-server-netty:$ktor_version")
    implementation("io.ktor:ktor-server-content-negotiation:$ktor_version")
    implementation("io.ktor:ktor-server-status-pages:$ktor_version")

    // JSON Serialization (Jackson)
    implementation("com.fasterxml.jackson.core:jackson-databind:2.15.0")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.15.0")
    implementation("io.ktor:ktor-serialization-jackson:$ktor_version")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.15.0")

    // Logging
    implementation("ch.qos.logback:logback-classic:1.4.12")
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(11)
}
