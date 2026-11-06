# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import json
import logging
import ssl
import sys

import kafka
import requests

from DBImportConfig import common_config


class sendStatistics:
    def __init__(self):
        logging.debug("Executing rest.__init__()")

        self.common_config = common_config.config()
        self.RESTheaders = {"Content-type": "application/json"}

        self.RESTendpoint = self.common_config.getConfigValue(key="rest_url")
        self.RESTtimeout = self.common_config.getConfigValue(key="rest_timeout")
        self.RESTverifySSL = self.common_config.getConfigValue(key="rest_verifyssl")
        self.RESTtrustCAfile = self.common_config.getConfigValue(key="rest_trustcafile")

        self.kafkaBrokers = self.common_config.getConfigValue(key="kafka_brokers")
        self.kafkaTrustCAFile = self.common_config.getConfigValue(
            key="kafka_trustcafile"
        )
        self.kafkaSecurityProtocol = self.common_config.getConfigValue(
            key="kafka_securityprotocol"
        )
        self.kafkaSaslMechanism = self.common_config.getConfigValue(
            key="kafka_saslmechanism"
        )
        self.kafkaTopic = self.common_config.getConfigValue(key="kafka_topic")

        self.awsSNStopic = self.common_config.getConfigValue(
            key="post_data_to_awssns_topic"
        )

        if self.RESTendpoint == "":
            logging.error(
                "Cant find the REST endpoint. Please check configuration file"
            )
            sys.exit(1)

        if self.RESTtimeout == "":
            logging.error(
                "Cant find the REST endpoint timeout. Please check configuration file"
            )
            sys.exit(1)

    def sendRESTdata(self, json_data):
        """Sending data to REST endpoint"""
        logging.debug("Executing sendStatistics.sendRESTdata")

        logging.info("Sending data to REST interface")
        logging.debug(
            "Sending the following JSON to REST: %s"
            % (json.dumps(json_data, sort_keys=True, indent=4))
        )

        response_code = -1

        try:
            rest_protocol = self.RESTendpoint.split(":")[0].lower()

            if rest_protocol == "http":
                response = requests.post(
                    self.RESTendpoint,
                    data=json_data,
                    headers=self.RESTheaders,
                    timeout=self.RESTtimeout,
                )
            else:
                if self.RESTverifySSL == True:
                    rest_verify = self.RESTtrustCAfile
                else:
                    rest_verify = False

                response = requests.post(
                    self.RESTendpoint,
                    data=json_data,
                    headers=self.RESTheaders,
                    timeout=self.RESTtimeout,
                    verify=rest_verify,
                )

            response_code = response.status_code
        except requests.exceptions.RequestException as e:
            logging.error(e)

        if response_code != 200:
            logging.error("REST call unsuccessful! Return code %s" % (response_code))

        logging.debug("Executing sendStatistics.sendRESTdata - Finished")
        return response_code

    def publishKafkaData(self, json_data):
        """Publish data to Kafka topic"""
        logging.debug("Executing sendStatistics.publishKafkaData")

        logging.info("Sending statistics data to Kafka topic")
        logging.debug("Sending the following JSON to Kafka: %s" % (json_data))
        kafka_logger = logging.getLogger("kafka")
        kafka_logger.setLevel(logging.ERROR)

        kafka_error = False
        result = True
        # 		logging.error("SEND TO KAFKA DISABLED")
        # 		return result

        try:
            ssl_context = ssl.create_default_context(cafile=self.kafkaTrustCAFile)

        except FileNotFoundError:
            logging.error("SSL CA file for Kafka cant be found")
            ssl_context = None

        try:
            if self.kafkaSaslMechanism != None and self.kafkaSecurityProtocol != None:
                producer = kafka.KafkaProducer(
                    bootstrap_servers=self.kafkaBrokers,
                    security_protocol=self.kafkaSecurityProtocol,
                    sasl_mechanism=self.kafkaSaslMechanism,
                    ssl_context=ssl_context,
                )
            else:
                producer = kafka.KafkaProducer(
                    bootstrap_servers=self.kafkaBrokers, ssl_context=ssl_context
                )

            producer.send(topic=self.kafkaTopic, value=json_data.encode("utf-8"))

        except AssertionError as e:
            logging.error("Kafka Error: %s" % e)
            kafka_error = True

        except kafka.errors.NoBrokersAvailable:
            logging.error("None of the Kafka brokers configured are available")
            kafka_error = True

        except kafka.errors.UnrecognizedBrokerVersion:
            logging.error(
                "Unrecognized broker version. Are you connecting to a secure Kafka broker without the correct security protocols?"
            )
            kafka_error = True

        except kafka.errors.KafkaTimeoutError:
            logging.error("Timeout error when trying to send statistics to Kafka")
            kafka_error = True

        except ssl.SSLCertVerificationError:
            logging.error("SSL Certificate error when communicating with Kafka")
            kafka_error = True

        if kafka_error == True:
            result = False

        logging.debug("Executing sendStatistics.publishKafkaData - Finished")
        return result

    def sendAWSSNSdata(self, json_data):
        """Publish data to AWS SNS topic"""
        logging.debug("Executing sendStatistics.sendAWSSNSdata")

        logging.info("Sending statistics data to AWS SNS topic")
        logging.debug("Sending the following JSON to AWS SNS: %s" % (json_data))

        result = True

        import boto3
        from botocore.exceptions import ClientError

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name="sns", region_name=self.common_config.awsRegion
        )

        try:
            publish_sns_response = client.publish(
                TopicArn=self.awsSNStopic, Message=json_data
            )
        except ClientError as e:  # For a list of exceptions thrown, see
            # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
            logging.error(e)
            result = False

        logging.debug("Executing sendStatistics.sendAWSSNSdata - Finished")
        return result
