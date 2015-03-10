/*
 * Copyright 2014 Jason Woods.
 *
 * This file is a modification of code from Logstash Forwarder.
 * Copyright 2012-2013 Jordan Sissel and contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package transports

import (
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/driskell/log-courier/src/lc-lib/core"
	"sync"
)

type TransportKafka08Registrar struct {
}

type TransportKafka08Factory struct {
	transport string

	KafkaTopic       string `config:"kafka topic"`
	KafkaPartitioner string `config:"kafka partitioner"`
	KafkaCompression string `config:"kafka compression"`
	KafkaClientID    string `config:"kafka clientid"`
}

type TransportKafka08 struct {
	config     *TransportKafka08Factory
	net_config *core.NetworkConfig

	wait     sync.WaitGroup
	shutdown chan interface{}

	producer                sarama.Producer
	producer_config         *sarama.Config
	partitioner_constructor sarama.PartitionerConstructor
	compression_codec       sarama.CompressionCodec

	send_chan chan []byte
	recv_chan chan interface{}
	can_send  chan int
}

// If you use the hash partitioner, the "kafka_key" field is extracted and used
type KafkaKey struct {
	KafkaKey string `json:"kafka_key"`
}

func NewKafka08TransportFactory(config *core.Config, config_path string, unused map[string]interface{}, name string) (core.TransportFactory, error) {
	var err error

	ret := &TransportKafka08Factory{
		transport: name,
	}

	if err = config.PopulateConfig(ret, config_path, unused); err != nil {
		return nil, err
	}
	if err = config.ReportUnusedConfig(config_path, unused); err != nil {
		return nil, err
	}

	return ret, nil
}

func (f *TransportKafka08Factory) NewTransport(config *core.NetworkConfig) (core.Transport, error) {
	ret := &TransportKafka08{
		config:     f,
		net_config: config,
	}
	if len(ret.config.KafkaClientID) <= 0 {
		log.Debug("Defaulting Kafka ClientID to log-courier")
		ret.config.KafkaClientID = "log-courier"
	} else {
		log.Debug("Kafka ClientID: %s", ret.config.KafkaClientID)
	}
	partitioner := ret.config.KafkaPartitioner
	if partitioner == "round robin" {
		ret.partitioner_constructor = sarama.NewRoundRobinPartitioner
	} else if partitioner == "random" {
		ret.partitioner_constructor = sarama.NewRandomPartitioner
	} else if partitioner == "hash" {
		ret.partitioner_constructor = sarama.NewHashPartitioner
	} else {
		log.Warning("wrong value for partitioner, can only take 'round robin', 'random' or 'hash'. Defaulting to 'round robin' now.")
		ret.partitioner_constructor = sarama.NewRoundRobinPartitioner
	}
	compression_codec := ret.config.KafkaCompression
	if compression_codec == "none" {
		ret.compression_codec = sarama.CompressionNone
	} else if compression_codec == "snappy" {
		ret.compression_codec = sarama.CompressionSnappy
	} else if compression_codec == "gzip" {
		ret.compression_codec = sarama.CompressionGZIP
	} else {
		log.Warning("wrong value for compression codec, can only take 'none', 'snappy' or 'gzip'. Defaulting to 'none' now.")
		ret.compression_codec = sarama.CompressionNone
	}
	return ret, nil
}

func (t *TransportKafka08) ReloadConfig(new_net_config *core.NetworkConfig) int {
	// seriously ... just restart the app, it is really lightweight after all
	return core.Reload_None
}

func (t *TransportKafka08) Init() error {
	if t.shutdown != nil {
		t.disconnect()
	}
	// Signal channels
	t.shutdown = make(chan interface{}, 1)
	t.send_chan = make(chan []byte, 1)
	t.recv_chan = make(chan interface{}, 1)
	t.can_send = make(chan int, 1)
	t.can_send <- 1

	// Initialize Kafka Producer
	config := sarama.NewConfig()
	config.ClientID = t.config.KafkaClientID
	config.Producer.AckSuccesses = false
	config.Producer.RequiredAcks = sarama.NoResponse
	config.Producer.Partitioner = t.partitioner_constructor
	config.Producer.Compression = t.compression_codec
	t.producer_config = config

	producer, err := sarama.NewProducer(t.net_config.Servers, config)
	if err != nil {
		return err
	}
	t.producer = producer

	t.wait.Add(1)
	go t.sender()

	log.Info("Transport 'kafka08' initialized: producer created and online")
	return nil
}

func (t *TransportKafka08) kafkaReconnect() {
	err := t.producer.Close()
	if err != nil {
		log.Error("Kafka Producer close error: '%s'", err.Error())
	}
	p, err := sarama.NewProducer(t.net_config.Servers, t.producer_config)
	if err != nil {
		log.Error("Error creating new Kafka producer: '%s'", err.Error())
	}
	t.producer = p
}

func (t *TransportKafka08) sender() {
SendLoop:
	for {
		select {
		case <-t.shutdown:
			// Shutdown
			break SendLoop
		case msg := <-t.send_chan:
			// Ask for more while we send this
			t.setChan(t.can_send)
			// Write deadline is managed by our net.Conn wrapper that tls will call into
			s := string(msg[:])
			var key sarama.StringEncoder
			if t.config.KafkaPartitioner == "hash" {
				var m KafkaKey
				err := json.Unmarshal(msg, &m)
				if err != nil {
					log.Debug("Could not unmarshal: %s", err.Error())
				} else {
					log.Debug("Kafka Hash Partitioner: key received: %+v", m)
					key = sarama.StringEncoder(m.KafkaKey)
				}
			}
			select {
			case t.producer.Input() <- &sarama.ProducerMessage{Topic: t.config.KafkaTopic, Key: key, Value: sarama.StringEncoder(s)}:
				log.Debug("> message queued to Kafka: %s", s)
			case err := <-t.producer.Errors():
				log.Error("Error received from Kafka producer: '%s'", err.Error())
				log.Info("Reconnecting to Kafka producer")
				t.kafkaReconnect()
				//t.recv_chan <- err
				//break SendLoop
			}
		}
	}

	t.wait.Done()
}

func (t *TransportKafka08) disconnect() {
	if t.shutdown == nil {
		return
	}

	// Send shutdown request
	close(t.shutdown)
	t.wait.Wait()
	t.shutdown = nil

	// Close kafka producer
	if err := t.producer.Close(); err != nil {
		log.Critical("Kafka producer could not be closed: %s", err.Error())
	}
}

func (t *TransportKafka08) setChan(set chan<- int) {
	select {
	case set <- 1:
	default:
	}
}

func (t *TransportKafka08) CanSend() <-chan int {
	return t.can_send
}

func (t *TransportKafka08) Write(signature string, message []byte) (err error) {
	t.send_chan <- message
	return nil
}

func (t *TransportKafka08) Read() <-chan interface{} {
	return t.recv_chan
}

func (t *TransportKafka08) Shutdown() {
	t.disconnect()
}

// Register the transports
func init() {
	core.RegisterTransport("kafka08", NewKafka08TransportFactory)
}
