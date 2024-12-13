plugins {
    kotlin("jvm") version "2.0.21"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven {
        url = uri("https://repo.osgeo.org/repository/release")
    }

}


dependencies {
    testImplementation(kotlin("test"))
    implementation("org.mongodb:mongodb-driver-sync:4.11.0")

    implementation ("com.opencsv:opencsv:5.5.2")
    implementation("org.slf4j:slf4j-simple:2.0.10")
    implementation ("org.locationtech.proj4j:proj4j:1.1.0")
    // YAML parsers
    implementation("com.fasterxml.jackson.core:jackson-databind:2.15.0")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.15.0")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.15.0")
}

tasks.test {
    useJUnitPlatform()
}

// configuring the kotlin plugin to use jdk 11 for compiling kotlin codel
kotlin {
    jvmToolchain(11)
}
