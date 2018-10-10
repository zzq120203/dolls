import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.2.50"
}

group = "cn.ca.iie.dolls"
version = "2.0"

repositories {
    jcenter()
}

dependencies {
    compile(kotlin("stdlib-jdk8"))

    //db
    compile("com.alibaba", "druid", "1.1.8")

    //mq
    compile("org.apache.rocketmq:rocketmq-client:4.3.0")

    //session
    compile("redis.clients:jedis:2.9.0")
    compile("org.eclipse.jetty:jetty-server:9.4.12.v20180830")

    //config
    compile("com.google.code.gson:gson:2.8.5")
    compile("commons-io:commons-io:2.6")

    //iie mq
    compile("com.alibaba.rocketmq:rocketmq-client:3.1.8")
    compile("org.apache.avro:avro:1.8.0")
    compile("com.google.guava:guava:24.0-jre")
    compile("org.apache.logging.log4j:log4j-slf4j-impl:2.10.0")
    compile("org.apache.logging.log4j:log4j-core:2.10.0")
    compile(fileTree("libs") {
        include("*.jar")
    })
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}