plugins {
    kotlin("jvm") version "1.9.24"
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
    implementation("org.locationtech.geomesa:geomesa-tools_2.12:5.1.0")
    implementation("com.opencsv:opencsv:5.5.2")
    implementation("org.locationtech.geomesa:geomesa-accumulo-datastore_2.12:5.1.0")
    implementation("org.locationtech.geomesa:geomesa-utils_2.12:5.1.0")
    implementation("org.locationtech.geomesa:geomesa-z3_2.12:5.1.0")
    implementation("org.locationtech.geomesa:geomesa-index-api_2.12:5.1.0")
    implementation("org.locationtech.geomesa:geomesa-filter_2.12:5.1.0")
    implementation("org.apache.accumulo:accumulo-core:2.1.3")
    implementation("org.slf4j:slf4j-simple:2.0.10")


}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(11)
}