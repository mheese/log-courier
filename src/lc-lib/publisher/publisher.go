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

package publisher

import (
	"errors"
	"github.com/driskell/log-courier/src/lc-lib/core"
	"github.com/driskell/log-courier/src/lc-lib/registrar"
	"sync"
	"time"
)

var (
	ErrNetworkTimeout = errors.New("Server did not respond within network timeout")
	ErrNetworkPing    = errors.New("Server did not respond to PING")
)

const (
	// TODO(driskell): Make the idle timeout configurable like the network timeout is?
	keepalive_timeout time.Duration = 900 * time.Second
)

const (
	Status_Disconnected = iota
	Status_Connected
	Status_Reconnecting
)

type NullEventSpool struct {
}

func newNullEventSpool() *NullEventSpool {
	return &NullEventSpool{}
}

func (s *NullEventSpool) Close() {
}

func (s *NullEventSpool) Add(event registrar.EventProcessor) {
}

func (s *NullEventSpool) Send() {
}

type Publisher struct {
	core.PipelineSegment
	core.PipelineConfigReceiver
	core.PipelineSnapshotProvider

	sync.RWMutex

	config    *core.NetworkConfig
	transport core.Transport
	status    int
	can_send  <-chan int
	//pending_ping     bool
	//pending_payloads map[string]*pendingPayload
	//first_payload    *pendingPayload
	//last_payload     *pendingPayload
	num_payloads    int64
	out_of_sync     int
	input           chan []*core.EventDescriptor
	registrar_spool registrar.EventSpooler
	shutdown        bool
	line_count      int64
	retry_count     int64
	seconds_no_ack  int

	timeout_count    int64
	line_speed       float64
	last_line_count  int64
	last_retry_count int64
	last_measurement time.Time
}

func NewPublisher(pipeline *core.Pipeline, config *core.NetworkConfig, registrar registrar.Registrator) (*Publisher, error) {
	ret := &Publisher{
		config: config,
		input:  make(chan []*core.EventDescriptor, 1),
	}

	if registrar == nil {
		ret.registrar_spool = newNullEventSpool()
	} else {
		ret.registrar_spool = registrar.Connect()
	}

	if err := ret.init(); err != nil {
		return nil, err
	}

	pipeline.Register(ret)

	return ret, nil
}

func (p *Publisher) init() error {
	var err error

	//p.pending_payloads = make(map[string]*pendingPayload)

	// Set up the selected transport
	if err = p.loadTransport(); err != nil {
		return err
	}

	return nil
}

func (p *Publisher) loadTransport() error {
	transport, err := p.config.TransportFactory.NewTransport(p.config)
	if err != nil {
		return err
	}

	p.transport = transport

	return nil
}

func (p *Publisher) Connect() chan<- []*core.EventDescriptor {
	return p.input
}

func (p *Publisher) Run() {
	defer func() {
		p.Done()
	}()

	var input_toggle <-chan []*core.EventDescriptor
	var err error
	var reload int

	timer := time.NewTimer(keepalive_timeout)
	stats_timer := time.NewTimer(time.Second)

	control_signal := p.OnShutdown()
	delay_shutdown := func() {
		// Flag shutdown for when we finish pending payloads
		// TODO: Persist pending payloads and resume? Quicker shutdown
		log.Warning("Delaying shutdown to wait for pending responses from the server")
		control_signal = nil
		p.shutdown = true
		p.can_send = nil
		input_toggle = nil
	}

PublishLoop:
	for {
		// Do we need to reload transport?
		if reload == core.Reload_Transport {
			// Shutdown and reload transport
			p.transport.Shutdown()

			if err = p.loadTransport(); err != nil {
				log.Error("The new transport configuration failed to apply: %s", err)
			}

			reload = core.Reload_None
		} else if reload != core.Reload_None {
			reload = core.Reload_None
		}

		if err = p.transport.Init(); err != nil {
			log.Error("Transport init failed: %s", err)

			now := time.Now()
			reconnect_due := now.Add(p.config.Reconnect)

		ReconnectTimeLoop:
			for {

				select {
				case <-time.After(reconnect_due.Sub(now)):
					break ReconnectTimeLoop
				case <-control_signal:
					// TODO: Persist pending payloads and resume? Quicker shutdown
					if p.num_payloads == 0 {
						break PublishLoop
					}

					delay_shutdown()
				case config := <-p.OnConfig():
					// Apply and check for changes
					reload = p.reloadConfig(&config.Network)

					// If a change and no pending payloads, process immediately
					if reload != core.Reload_None && p.num_payloads == 0 {
						break ReconnectTimeLoop
					}
				}

				now = time.Now()
				if now.After(reconnect_due) {
					break
				}
			}

			continue
		}

		p.Lock()
		p.status = Status_Connected
		p.Unlock()

		timer.Reset(keepalive_timeout)
		stats_timer.Reset(time.Second)

		//p.pending_ping = false
		input_toggle = nil
		p.can_send = p.transport.CanSend()

	SelectLoop:
		for {
			select {
			case <-p.can_send:
				// No pending payloads, are we shutting down? Skip if so
				if p.shutdown {
					break
				}

				log.Debug("Send now open: Awaiting events for new payload")

				// Enable event wait
				input_toggle = p.input
			case events := <-input_toggle:
				log.Debug("Sending new payload of %d events", len(events))

				// Send
				if err = p.sendNewPayload(events); err != nil {
					break SelectLoop
				}

				// Wait for send signal again
				input_toggle = nil
			case data := <-p.transport.Read():
				switch data.(type) {
				case error:
					err = data.(error)
					log.Error("Error received from transport: %s", err.Error())
					break SelectLoop
				default:
					log.Warning("unknown data received from transport")
					break SelectLoop
				}
			case <-control_signal:
				// If no pending payloads, simply end
				if p.num_payloads == 0 {
					break PublishLoop
				}

				delay_shutdown()
			case config := <-p.OnConfig():
				// Apply and check for changes
				reload = p.reloadConfig(&config.Network)

				// If a change and no pending payloads, process immediately
				if reload != core.Reload_None && p.num_payloads == 0 {
					break SelectLoop
				}

				p.can_send = nil
			case <-stats_timer.C:
				p.updateStatistics(Status_Connected, nil)
				stats_timer.Reset(time.Second)
			}
		}

		if err != nil {
			// If we're shutting down and we hit a timeout and aren't out of sync
			// We can then quit - as we'd be resending payloads anyway
			if p.shutdown && p.out_of_sync == 0 {
				log.Error("Transport error: %s", err)
				break PublishLoop
			}

			p.updateStatistics(Status_Reconnecting, err)

			// An error occurred, reconnect after timeout
			log.Error("Transport error, will try again: %s", err)
			time.Sleep(p.config.Reconnect)
		} else {
			log.Info("Reconnecting transport")

			p.updateStatistics(Status_Reconnecting, nil)
		}

	}

	p.transport.Shutdown()

	// Disconnect from registrar
	p.registrar_spool.Close()

	log.Info("Publisher exiting")
}

func (p *Publisher) reloadConfig(new_config *core.NetworkConfig) int {
	old_config := p.config
	p.config = new_config

	// Transport reload will return whether we need a full reload or not
	reload := p.transport.ReloadConfig(new_config)
	if reload == core.Reload_Transport {
		return core.Reload_Transport
	}

	// Same servers?
	if len(new_config.Servers) != len(old_config.Servers) {
		return core.Reload_Servers
	}

	for i := range new_config.Servers {
		if new_config.Servers[i] != old_config.Servers[i] {
			return core.Reload_Servers
		}
	}

	return reload
}

func (p *Publisher) updateStatistics(status int, err error) {
	p.Lock()

	p.status = status

	p.line_speed = core.CalculateSpeed(time.Since(p.last_measurement), p.line_speed, float64(p.line_count-p.last_line_count), &p.seconds_no_ack)

	p.last_line_count = p.line_count
	p.last_retry_count = p.retry_count
	p.last_measurement = time.Now()

	if err == ErrNetworkTimeout || err == ErrNetworkPing {
		p.timeout_count++
	}

	p.Unlock()
}

func (p *Publisher) sendNewPayload(events []*core.EventDescriptor) (err error) {
	for _, event := range events {
		p.transport.Write("JDAT", event.Event)
	}
	p.registrar_spool.Add(registrar.NewAckEvent(events))
	p.registrar_spool.Send()
	return nil
}

func (p *Publisher) Snapshot() []*core.Snapshot {
	p.RLock()

	snapshot := core.NewSnapshot("Publisher")

	switch p.status {
	case Status_Connected:
		snapshot.AddEntry("Status", "Connected")
	case Status_Reconnecting:
		snapshot.AddEntry("Status", "Reconnecting...")
	default:
		snapshot.AddEntry("Status", "Disconnected")
	}

	snapshot.AddEntry("Speed (Lps)", p.line_speed)
	snapshot.AddEntry("Published lines", p.last_line_count)
	snapshot.AddEntry("Pending Payloads", p.num_payloads)
	snapshot.AddEntry("Timeouts", p.timeout_count)
	snapshot.AddEntry("Retransmissions", p.last_retry_count)

	p.RUnlock()

	return []*core.Snapshot{snapshot}
}
