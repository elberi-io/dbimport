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


class SendStatistics:
    def __init__(self) -> None:
        logging.debug("Executing rest.__init__()")

        self.common_config = common_config.Config()
        self.rest_headers = {"Content-type": "application/json"}

        self.rest_endpoint = self.common_config.get_config_value(key="rest_url")
        self.rest_timeout = self.common_config.get_config_value(key="rest_timeout")
        self.rest_verify_ssl = self.common_config.get_config_value(key="rest_verifyssl")
        self.rest_trust_ca_file = self.common_config.get_config_value(
            key="rest_trustcafile"
        )

        self.kafka_brokers = self.common_config.get_config_value(key="kafka_brokers")
        self.kafka_trust_ca_file = self.common_config.get_config_value(
            key="kafka_trustcafile"
        )
        self.kafka_security_protocol = self.common_config.get_config_value(
            key="kafka_securityprotocol"
        )
        self.kafka_sasl_mechanism = self.common_config.get_config_value(
            key="kafka_saslmechanism"
        )
        self.kafka_topic = self.common_config.get_config_value(key="kafka_topic")

        self.aws_sns_topic = self.common_config.get_config_value(
            key="post_data_to_awssns_topic"
        )

        if self.rest_endpoint == "":
            logging.error(
                "Cant find the REST endpoint. Please check configuration file"
            )
            sys.exit(1)

        if self.rest_timeout == "":
            logging.error(
                "Cant find the REST endpoint timeout. Please check configuration file"
            )
            sys.exit(1)

    def send_rest_data(self, json_data: str) -> int:
        """Sending data to REST endpoint"""
        logging.debug("Executing sendStatistics.send_rest_data")

        logging.info("Sending data to REST interface")
        logging.debug(
            "Sending the following JSON to REST: %s",
            json.dumps(json_data, sort_keys=True, indent=4),
        )

        response_code = -1

        try:
            rest_protocol = self.rest_endpoint.split(":")[0].lower()

            if rest_protocol == "http":
                response = requests.post(
                    self.rest_endpoint,
                    data=json_data,
                    headers=self.rest_headers,
                    timeout=self.rest_timeout,
                )
            else:
                if self.rest_verify_ssl == True:
                    rest_verify = self.rest_trust_ca_file
                else:
                    rest_verify = False

                response = requests.post(
                    self.rest_endpoint,
                    data=json_data,
                    headers=self.rest_headers,
                    timeout=self.rest_timeout,
                    verify=rest_verify,
                )

            response_code = response.status_code
        except requests.exceptions.RequestException as e:
            logging.error(e)

        if response_code != 200:
            logging.error("REST call unsuccessful! Return code %s", response_code)

        logging.debug("Executing sendStatistics.send_rest_data - Finished")
        return response_code

    def publish_kafka_data(self, json_data: str) -> bool:
        """Publish data to Kafka topic"""
        logging.debug("Executing sendStatistics.publish_kafka_data")

        logging.info("Sending statistics data to Kafka topic")
        logging.debug("Sending the following JSON to Kafka: %s", json_data)
        kafka_logger = logging.getLogger("kafka")
        kafka_logger.setLevel(logging.ERROR)

        kafka_error = False
        result = True
        # 		logging.error("SEND TO KAFKA DISABLED")
        # 		return result

        try:
            ssl_context = ssl.create_default_context(cafile=self.kafka_trust_ca_file)

        except FileNotFoundError:
            logging.error("SSL CA file for Kafka cant be found")
            ssl_context = None

        try:
            if (
                self.kafka_sasl_mechanism != None
                and self.kafka_security_protocol != None
            ):
                producer = kafka.KafkaProducer(
                    bootstrap_servers=self.kafka_brokers,
                    security_protocol=self.kafka_security_protocol,
                    sasl_mechanism=self.kafka_sasl_mechanism,
                    ssl_context=ssl_context,
                )
            else:
                producer = kafka.KafkaProducer(
                    bootstrap_servers=self.kafka_brokers, ssl_context=ssl_context
                )

            producer.send(topic=self.kafka_topic, value=json_data.encode("utf-8"))

        except AssertionError as e:
            logging.error("Kafka Error: %s", e)
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

        if kafka_error:
            result = False

        logging.debug("Executing sendStatistics.publish_kafka_data - Finished")
        return result

    def send_aws_sns_data(self, json_data: str) -> bool:
        """Publish data to AWS SNS topic"""
        logging.debug("Executing sendStatistics.send_aws_sns_data")

        logging.info("Sending statistics data to AWS SNS topic")
        logging.debug("Sending the following JSON to AWS SNS: %s", json_data)

        result = True

        import boto3
        from botocore.exceptions import ClientError

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name="sns", region_name=self.common_config.aws_region
        )

        try:
            publish_sns_response = client.publish(
                TopicArn=self.aws_sns_topic, Message=json_data
            )
        except ClientError as e:  # For a list of exceptions thrown, see
            # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
            logging.error(e)
            result = False

        logging.debug("Executing sendStatistics.send_aws_sns_data - Finished")
        return result
