FROM apache/spark:3.5.7-scala2.12-java17-python3-ubuntu

USER root

ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_JAVA_SDK_BUNDLE_VERSION=1.12.262

RUN set -eux; \
    curl --fail --location --silent --show-error \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
      --output "/opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    curl --fail --location --silent --show-error \
      "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_JAVA_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_JAVA_SDK_BUNDLE_VERSION}.jar" \
      --output "/opt/spark/jars/aws-java-sdk-bundle-${AWS_JAVA_SDK_BUNDLE_VERSION}.jar"; \
    test -s "/opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    test -s "/opt/spark/jars/aws-java-sdk-bundle-${AWS_JAVA_SDK_BUNDLE_VERSION}.jar"; \
    chmod 0444 \
      "/opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
      "/opt/spark/jars/aws-java-sdk-bundle-${AWS_JAVA_SDK_BUNDLE_VERSION}.jar"

USER spark
