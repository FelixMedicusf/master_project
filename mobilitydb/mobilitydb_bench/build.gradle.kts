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
    implementation("org.postgresql:postgresql:42.6.0")
    implementation ("com.opencsv:opencsv:5.5.2")
    implementation("org.slf4j:slf4j-simple:2.0.10")
    implementation ("org.locationtech.proj4j:proj4j:1.1.0")
}

tasks.test {
    useJUnitPlatform()
}

// configuring the kotlin plugin to use jdk 11 for compiling kotlin code
kotlin {
    jvmToolchain(11)
}
