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

    compile("com.alibaba", "druid", "1.1.8")

    compile("org.apache.rocketmq:rocketmq-client:4.3.0")
    compile("redis.clients:jedis:2.9.0")
    compile("org.eclipse.jetty:jetty-server:9.4.12.v20180830")

    compile("com.google.code.gson:gson:2.8.5")
    compile("commons-io:commons-io:2.6")

}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}