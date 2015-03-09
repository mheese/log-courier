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
	"github.com/driskell/log-courier/src/lc-lib/core"
	"sync"
)

type TransportKafka08Registrar struct {
}

type TransportKafka08Factory struct {
	transport string

	KafkaBrokers string `config:"kafka brokers"`
	KafkaTopic   string `config:"kafka topic"`
}

type TransportKafka08 struct {
	config     *TransportKafka08Factory
	net_config *core.NetworkConfig

	wait     sync.WaitGroup
	shutdown chan interface{}

	send_chan chan []byte

	can_send chan int
}

func NewKafka08TransportFactory(config *core.Config, config_path string, unused map[string]interface{}, name string) (core.TransportFactory, error) {
	var err error

	ret := &TransportKafka08Factory{
		transport: name,
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

	return ret, nil
}

func (t *TransportKafka08) ReloadConfig(new_net_config *core.NetworkConfig) int {
	// Check we can grab new TCP config to compare, if not force transport reinit
	//new_config, ok := new_net_config.TransportFactory.(*TransportKafka08Factory)
	//if !ok {
	//	return core.Reload_Transport
	//}

	// Publisher handles changes to net_config, but ensure we store the latest in case it asks for a reconnect
	//t.net_config = new_net_config

	return core.Reload_None
}

func (t *TransportKafka08) Init() error {
	if t.shutdown != nil {
		t.disconnect()
	}
	// Signal channels
	t.shutdown = make(chan interface{}, 1)
	t.send_chan = make(chan []byte, 1)
	t.can_send = make(chan int, 1)
	t.can_send <- 1

	t.wait.Add(1)
	go t.sender()

	log.Debug("Kafka08 initialized")
	return nil
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
			log.Debug(s)
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
	//return t.recv_chan
	return nil
}

func (t *TransportKafka08) Shutdown() {
	t.disconnect()
}

// Register the transports
func init() {
	core.RegisterTransport("kafka08", NewKafka08TransportFactory)
}
