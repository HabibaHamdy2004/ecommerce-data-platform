FROM apache/spark:3.5.3-python3

USER root

# =========================================================
# Hadoop S3A support for MinIO
# =========================================================

RUN curl -L \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -o /opt/spark/jars/hadoop-aws-3.3.4.jar

RUN curl -L \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar


# =========================================================
# Apache Iceberg
# Spark 3.5 + Scala 2.12
# =========================================================

RUN curl -L \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.0/iceberg-spark-runtime-3.5_2.12-1.10.0.jar \
    -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.10.0.jar

RUN curl -L \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.0/iceberg-aws-bundle-1.10.0.jar \
    -o /opt/spark/jars/iceberg-aws-bundle-1.10.0.jar


# =========================================================
# Project scripts
# =========================================================

COPY pyspark/scripts /opt/spark/scripts

USER spark
