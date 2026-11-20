/**
 * flink-connect 빌드 스크립트
 *
 * - Kafka(JSON) → Postgres/MSSQL/Doris 적재용 Flink Job
 * - 같은 JAR + YAML 설정으로 멀티토픽 Job / 중요토픽 전용 Job 분리 운영
 * - Java 21, Flink 1.20
 *
 * 배포 JAR: Flink 본체는 클러스터 것을 쓰고,
 * 커넥터·드라이버·YAML/JSON 라이브러리만 shadow(fat) JAR에 넣는다.
 *
 * 테스트: ./gradlew test
 * 로컬:   ./gradlew run --args="config/example-pipeline.yaml"
 * 배포:   ./gradlew shadowJar  →  build/libs/flink-connect-0.1.0-all.jar
 */
plugins {
    java
    application
    id("com.gradleup.shadow") version "8.3.5"
}

group = "com.smdp"
version = "0.1.0"

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

repositories {
    mavenCentral()
}

val flinkVersion = "1.20.0"

// 클러스터가 제공하는 Flink — fat JAR에 넣지 않음
val flinkProvided: Configuration by configurations.creating

configurations {
    compileOnly.get().extendsFrom(flinkProvided)
    testImplementation.get().extendsFrom(flinkProvided)
}

dependencies {
    flinkProvided("org.apache.flink:flink-streaming-java:$flinkVersion")
    flinkProvided("org.apache.flink:flink-clients:$flinkVersion")
    flinkProvided("org.apache.flink:flink-connector-base:$flinkVersion")

    implementation("org.apache.flink:flink-connector-kafka:3.3.0-1.20") {
        exclude(group = "org.apache.flink", module = "flink-streaming-java")
        exclude(group = "org.apache.flink", module = "flink-connector-base")
        exclude(group = "org.apache.flink", module = "flink-core")
    }
    implementation("org.apache.flink:flink-connector-jdbc:3.3.0-1.20") {
        exclude(group = "org.apache.flink", module = "flink-streaming-java")
        exclude(group = "org.apache.flink", module = "flink-connector-base")
        exclude(group = "org.apache.flink", module = "flink-core")
    }
    // 공식 Doris Stream Load Sink (Flink 1.20)
    implementation("org.apache.doris:flink-doris-connector-1.20:26.3.0") {
        exclude(group = "org.apache.flink", module = "flink-streaming-java")
        exclude(group = "org.apache.flink", module = "flink-connector-base")
        exclude(group = "org.apache.flink", module = "flink-core")
        exclude(group = "org.apache.flink", module = "flink-table-api-java-bridge")
        exclude(group = "org.apache.flink", module = "flink-table-planner-loader")
    }
    implementation("org.apache.flink:flink-json:$flinkVersion") {
        exclude(group = "org.apache.flink", module = "flink-streaming-java")
        exclude(group = "org.apache.flink", module = "flink-core")
    }
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.2")
    implementation("org.yaml:snakeyaml:2.3")
    implementation("org.slf4j:slf4j-api:2.0.16")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.16")

    implementation("org.postgresql:postgresql:42.7.4")
    implementation("com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11")
    implementation("com.mysql:mysql-connector-j:9.1.0")

    testImplementation("org.junit.jupiter:junit-jupiter:5.11.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainClass.set("com.smdp.flinkconnect.job.PipelineJob")
}

tasks.test {
    useJUnitPlatform()
    // Flink Kryo + Java 21 모듈 제한 (fromData 직렬화)
    jvmArgs(
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.time=ALL-UNNAMED",
        "--add-opens=java.base/java.math=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    )
}

tasks.named<JavaExec>("run") {
    classpath += flinkProvided
}

tasks.shadowJar {
    archiveClassifier.set("all")
    mergeServiceFiles()
    manifest {
        attributes["Main-Class"] = "com.smdp.flinkconnect.job.PipelineJob"
    }
    dependencies {
        exclude(dependency("org.apache.flink:flink-streaming-java:.*"))
        exclude(dependency("org.apache.flink:flink-clients:.*"))
        exclude(dependency("org.apache.flink:flink-core:.*"))
        exclude(dependency("org.apache.flink:flink-runtime:.*"))
        exclude(dependency("org.apache.flink:flink-connector-base:.*"))
        exclude(dependency("org.apache.flink:flink-java:.*"))
        exclude(dependency("org.apache.flink:flink-file-sink-common:.*"))
    }
}

tasks.named("assemble") {
    dependsOn(tasks.shadowJar)
}

tasks.jar {
    archiveClassifier.set("thin")
}
