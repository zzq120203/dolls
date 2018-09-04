import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.2.50"
}

group = "cn.ca.iie.dolls"
version = "1.1"

repositories {
    jcenter()
}

dependencies {
    compile(kotlin("stdlib-jdk8"))

    compile("com.alibaba", "druid", "1.1.8")

    compile("org.apache.rocketmq:rocketmq-client:4.3.0")
    compile("redis.clients:jedis:2.9.0")
    compile("org.eclipse.jetty:jetty-server:9.4.12.v20180830")
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}