Feature: Quick starts for OpenShift Streams for Apache Kafka

  A quick start is a guided tutorial with user tasks. OpenShift Streams for Apache Kafka provides several quick starts to help you get started quickly with the main features and functionality of the service.

  Scenario: Getting started with Red Hat OpenShift Streams for Apache Kafka
    Given you have a Red Hat account

    # 1. Creating a Kafka instance in OpenShift Streams for Apache Kafka
    Given you are logged in to the OpenShift Streams for Apache Kafka
    When you create a Kafka instance with a unique name
    Then the Kafka instance is listed in the instances table
    And the Kafka instance is shown as Ready

    # 2. Creating a service account to connect to a Kafka instance in OpenShift Streams for Apache Kafka
    Given you have created a Kafka instance in OpenShift Streams for Apache Kafka
    * the Kafka instance is in Ready state
    * the Kafka instance has a generated bootstrap server
    When you create a service account with a unique name
    Then the service account has a generated Client ID and Client Secret
    And the service account is listed in the service accounts table

    # 3. Creating a Kafka topic in OpenShift Streams for Apache Kafka
    Given you have created a Kafka instance in OpenShift Streams for Apache Kafka
    * the Kafka instance is in Ready state
    When you create the Kafka topic test-topic
    Then the Kafka topic test-topic is listed in the topics table

  Scenario: Java test

    Given you have a Red Hat account
    * you are logged in to the OpenShift Streams for Apache Kafka
    * you have a running Kafka instance in OpenShift Streams for Apache Kafka

     # 1. Importing the Python sample code
    When you clone the rhosak_example_codes repository from GitHub examples for Java
    Then the rhosak_example_codes example repository for java is available locally

    # 2. Configuring the Quarkus example application to connect to a Kafka instance
    Given you have the bootstrap server endpoint for your Kafka instance
    * you have the generated credentials for your service account
    * you have the OAUTHBEARER token endpoint for the Kafka instance
    When you set the Kafka instance bootstrap server endpoint, service account credentials, and OAUTHBEARER token endpoint as environment variables for Java

    # 3. Creating the prices Kafka topic in OpenShift Streams for Apache Kafka
    Given you have a running Kafka instance in OpenShift Streams for Apache Kafka
    * the Kafka instance is in Ready state
    When you have created the Kafka topic prices
    Then the Kafka topic prices is listed in the topics table

    # 4. Running the Java client example producer
    Given the Kafka instance is in Ready state
    Given you have set the permissions for your service account to produce and consume from topic prices
    Then you run Java client example producer

    # 5. Running the Java client example consumer
    Given the Kafka instance is in Ready state
    When you run Java client example consumer
    Then Java client consumer should print proper message

  Scenario: Python test

    Given you have a Red Hat account
    * you are logged in to the OpenShift Streams for Apache Kafka
    * you have a running Kafka instance in OpenShift Streams for Apache Kafka

     # 1. Importing the Python sample code
    When you clone the rhosak_example_codes repository from GitHub examples
    Then the rhosak_example_codes example repository is available locally

    # 2. Configuring the Python example application to connect to a Kafka instance
    Given you have the bootstrap server endpoint for your Kafka instance
    * you have the generated credentials for your service account
    * you have the OAUTHBEARER token endpoint for the Kafka instance
    When you set the Kafka instance bootstrap server endpoint, service account credentials, and OAUTHBEARER token endpoint as environment variables for Python app

    # 3. Creating the prices Kafka topic in OpenShift Streams for Apache Kafka
    Given you have a running Kafka instance in OpenShift Streams for Apache Kafka
    * the Kafka instance is in Ready state
    When you have created the Kafka topic prices
    Then the Kafka topic prices is listed in the topics table

    # 4. Running the Python example producer
    Given the Kafka instance is in Ready state
    Given you have set the permissions for your service account to produce and consume from topic prices
    Then you run Python example producer

    # 4. Running the Python example consumer
    Given the Kafka instance is in Ready state
    When you run Python client consumer
    Then Python example consumer should print proper message

  Scenario: Spring boot example application test

    Given you have a Red Hat account
    * you are logged in to the OpenShift Streams for Apache Kafka
    * you have a running Kafka instance in OpenShift Streams for Apache Kafka

    # 1. Importing the Springboot sample code
    When you clone the rhosak_example_codes repository from GitHub examples
    Then the rhosak_example_codes example repository is available locally

    # 2. Configuring the Springboot example application to connect to a Kafka instance
    Given you have the bootstrap server endpoint for your Kafka instance
    * you have the generated credentials for your service account
    * you have the OAUTHBEARER token endpoint for the Kafka instance
    When you set the Kafka instance bootstrap server endpoint, service account credentials, and OAUTHBEARER token endpoint as environment variables for Spring app

    # 3. Creating the prices Kafka topic in OpenShift Streams for Apache Kafka
    Given you have a running Kafka instance in OpenShift Streams for Apache Kafka
    * the Kafka instance is in Ready state
    When you have created the Kafka topic prices
    Then the Kafka topic prices is listed in the topics table

    # 4. Running the springboot example producer
    Given the Kafka instance is in Ready state
    Given you have set the permissions for your service account to produce and consume from topic prices
    Then you run Springboot client example producer

    # 5. Running the springboot client example consumer
    Given the Kafka instance is in Ready state
    When you run Springboot client example consumer
    Then Springboot consumer should contain proper message
  
  Scenario: Using Quarkus applications with Kafka instances in Red Hat OpenShift Streams for Apache Kafka
    Given you have a Red Hat account
    * you are logged in to the OpenShift Streams for Apache Kafka
    * you have a running Kafka instance in OpenShift Streams for Apache Kafka

    # 1. Importing the Quarkus sample code
    When you clone the app-services-guides repository from GitHub
    Then the app-services-guides repository is available locally

    # 2. Configuring the Quarkus example application to connect to a Kafka instance
    Given you have the bootstrap server endpoint for your Kafka instance
    * you have the generated credentials for your service account
    * you have the OAUTHBEARER token endpoint for the Kafka instance
    When you set the Kafka instance bootstrap server endpoint, service account credentials, and OAUTHBEARER token endpoint as environment variables
#    Then the `application.properties` file of the Quarkus example application contains all required configurations to authenticate the Kafka instance

    # 3. Creating the prices Kafka topic in OpenShift Streams for Apache Kafka
    Given you have a running Kafka instance in OpenShift Streams for Apache Kafka
    * the Kafka instance is in Ready state
    When you have created the Kafka topic prices
    Then the Kafka topic prices is listed in the topics table

    # 4. Running the Quarkus example application
    Given the Kafka instance is in Ready state
    Given you have set the permissions for your service account to produce and consume from topic prices
    When you run Quarkus example applications
    Then the application is running and the `Last price` is updated at http localhost port 8080 resource prices.html


  Scenario: Using Node.js applications with Kafka instances in Red Hat OpenShift Streams for Apache Kafka
    Given you have a Red Hat account
    * you are logged in to the OpenShift Streams for Apache Kafka
    * you have a running Kafka instance in OpenShift Streams for Apache Kafka
    * the Kafka instance is in Ready state
    * Git is installed
    * You have an IDE such as IntelliJ IDEA, Eclipse, or VSCode
    * Node.js 14 is installed

    # 1. Importing the Node.js sample code
    When you clone the `reactive-example` repository from GitHub
    Then the `reactive-example` repository is available locally

    # 2. Configuring the Node.js example application to connect to a Kafka instance
    Given you have the bootstrap server endpoint and the service account credentials for the Kafka instance
    * You created a file called `.env` at the root level of the cloned `reactive-example` repository
    When you set the Kafka instance bootstrap server endpoint and service account credentials as environment variables in the `.env` file
    And you set the value of the `KAFKA_SASL_MECHANISM` environment variable to `plain` in the `.env` file
    Then the `reactive-example/.env` file of the Node.js example application contains all configurations required to authenticate the Kafka instance

    # 3. Creating a Kafka topic in OpenShift Streams for Apache Kafka
    Given you’ve created a Kafka instance in OpenShift Streams for Apache Kafka
    * the Kafka instance is in Ready state
    When you create a Kafka topic called `countries`
    Then the `countries` Kafka topic is listed in the topics table

    # 4. Running the Node.js example application
    Given you've configured the Node.js example application to connect to the Kafka instance
    * You’ve created a Kafka instance in OpenShift Streams for Apache Kafka
    * the Kafka instance is in Ready state
    * You've created the `countries` topic
    When you navigate to the `reactive-example/consumer-backend` directory of the cloned repository
    And you install the dependencies for the consumer component
    And you run the consumer component
    Then the consumer component is running but doesn't display country names on the command line
    Given you've opened a second command-line window or tab
    When you navigate to the `reactive-example/producer-backend` directory of the cloned repository
    And you install the dependencies for the producer component
    And you run the producer component
    Then the producer is running and displays country names
    And the consumer component displays the same country names as the producer on the first command line

  Scenario: Using Kcat with Kafka instances in Red Hat OpenShift Streams for Apache Kafka
    Given you have a Red Hat account
    * you are logged in to the OpenShift Streams for Apache Kafka
    * you have downloaded and verified the latest supported version of Kcat for your operating system
    * you have a running Kafka instance in OpenShift Streams for Apache Kafka

    # 1. Configuring Kcat to connect to a Kafka instance
    Given you have the bootstrap server endpoint for your Kafka instance
    * you have the generated credentials for your service account
    * you have set the permissions for your service account to access your Kafka instance resources
    When you set the Kafka instance bootstrap server endpoint and service account credentials as environment variables

    # 2. Producing messages in Kcat
    Given Kcat is installed
    * you have a running Kafka instance in OpenShift Streams for Apache Kafka
    * the Kafka instance is in Ready state
    * you have set the Kafka bootstrap server endpoint and your service account credentials as environment variables
    * you have created the Kafka topic my-first-kafka-topic
    When you start Kcat in producer mode on the topic my-first-kafka-topic
    And you enter messages into Kcat that you want to produce
    Then the producer is still running without any errors

    # 3. Consuming messages in Kcat
    Given Kcat is installed
    * you have a running Kafka instance in OpenShift Streams for Apache Kafka
    * the Kafka instance is in Ready state
    * you have set the Kafka bootstrap server endpoint and your service account credentials as environment variables
    * you have created the Kafka topic my-first-kafka-topic
    * you used a producer to produce example messages to a topic
    When you start Kcat in consumer mode on the topic my-first-kafka-topic
    Then your consumer is running without any errors
    And the consumer display the example messages from the producer

  Scenario: Using Kafka scripts to connect with Red Hat OpenShift Streams for Apache Kafka
    Given you have a Red Hat account
    * you are logged in to the OpenShift Streams for Apache Kafka
    * you have a running Kafka instance in OpenShift Streams for Apache Kafka
    * the Kafka instance has a generated bootstrap server
    * you have downloaded and verified the latest supported binary version of the Apache Kafka distribution

    # 1. Configuring Kafka scripts to connect to a Kafka instance
    Given you have the bootstrap server endpoint for your Kafka instance
    * you have the generated credentials for your service account
    When you create an `app-services.properties` file with SASL connection mechanism

    # 2. Producing messages using Kafka scripts
    Given you have a running Kafka instance in OpenShift Streams for Apache Kafka
    * the Kafka instance is in Ready state
    * you have set the permissions for your service account to access your Kafka instance resources
    * you have set the permissions for your service account to manipulate topic
    * the Kafka topic creation script `kafka-topics.sh` is available
    * the Kafka producer creation script `kafka-console-producer.sh` is available
    * an `app-services.properties` file is configured
    When you enter a command to create Kafka topic kafka-script-topic using `kafka-topics.sh`
    Then the topic kafka-script-topic is created in the Kafka instance
    When you produce messages to the topic kafka-script-topic using `kafka-console-producer.sh`
    Then the `kafka-console-producer` is still running without any errors

    # 3. Consuming messages using Kafka scripts
    Given you have a running Kafka instance in OpenShift Streams for Apache Kafka
    * the Kafka instance is in Ready state
    * the Kafka consumer creation script `kafka-console-consumer.sh` is available
    * an `app-services.properties` file is configured
    * you have the bootstrap server endpoint for your Kafka instance
    * you have created the Kafka topic kafka-script-topic
    * you used a `kafka-console-producer` to produce example messages to a topic
    When you consume messages from the topic kafka-script-topic you created using `kafka-console-consumer.sh`
    Then your `kafka-console-consumer` is running without any errors
    And the `kafka-console-consumer` display the example messages from the producer