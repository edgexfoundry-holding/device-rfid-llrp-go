//
// Copyright (C) 2020 Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"encoding"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.impcloud.net/RSP-Inventory-Suite/device-llrp-go/internal/llrp"
	"github.impcloud.net/RSP-Inventory-Suite/device-llrp-go/internal/retry"
	"io/ioutil"
	"net"
	"sync"
	"time"

	dsModels "github.com/edgexfoundry/device-sdk-go/pkg/models"
	"github.com/edgexfoundry/go-mod-core-contracts/clients/logger"
	contract "github.com/edgexfoundry/go-mod-core-contracts/models"
)

const (
	ServiceName string = "edgex-device-llrp"
)

var once sync.Once
var driver *Driver

type Driver struct {
	lc       logger.LoggingClient
	asyncCh  chan<- *dsModels.AsyncValues
	deviceCh chan<- []dsModels.DiscoveredDevice

	clients      map[string]*llrp.Client
	clientsMapMu sync.RWMutex

	svc ServiceWrapper
}

func NewProtocolDriver() dsModels.ProtocolDriver {
	once.Do(func() {
		driver = &Driver{
			clients: make(map[string]*llrp.Client),
		}
	})
	return driver
}

func (d *Driver) service() ServiceWrapper {
	if d.svc == nil {
		d.svc = RunningService()
	}
	return d.svc
}

// Initialize performs protocol-specific initialization for the device
// service.
func (d *Driver) Initialize(lc logger.LoggingClient, asyncCh chan<- *dsModels.AsyncValues, deviceCh chan<- []dsModels.DiscoveredDevice) error {
	if lc == nil {
		// prevent panics from this annoyance
		d.lc = logger.NewClientStdOut(ServiceName, false, "DEBUG")
		d.lc.Error("EdgeX initialized us with a nil logger >:(")
	} else {
		d.lc = lc
	}

	d.asyncCh = asyncCh
	d.deviceCh = deviceCh

	go func() {
		// hack: sleep to allow edgex time to finish loading cache and clients
		time.Sleep(5 * time.Second)

		d.addProvisionWatcher()
		// todo: check configuration to make sure discovery is enabled
		d.Discover()
	}()
	return nil
}

type protocolMap = map[string]contract.ProtocolProperties

const (
	ResourceReaderCap          = "ReaderCapabilities"
	ResourceReaderConfig       = "ReaderConfig"
	ResourceReaderNotification = "ReaderEventNotification"
	ResourceROSpec             = "ROSpec"
	ResourceROSpecID           = "ROSpecID"
	ResourceAccessSpec         = "AccessSpec"
	ResourceAccessSpecID       = "AccessSpecID"
	ResourceROAccessReport     = "ROAccessReport"

	ResourceAction = "Action"
	ActionDelete   = "Delete"
	ActionEnable   = "Enable"
	ActionDisable  = "Disable"
	ActionStart    = "Start"
	ActionStop     = "Stop"
)

// HandleReadCommands triggers a protocol Read operation for the specified device.
func (d *Driver) HandleReadCommands(devName string, p protocolMap, reqs []dsModels.CommandRequest) ([]*dsModels.CommandValue, error) {
	d.lc.Debug(fmt.Sprintf("LLRP-Driver.HandleWriteCommands: "+
		"device: %s protocols: %v reqs: %+v", devName, p, reqs))

	if len(reqs) == 0 {
		return nil, errors.New("missing requests")
	}

	c, err := d.getClient(devName, p)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	var responses = make([]*dsModels.CommandValue, len(reqs))
	for i := range reqs {
		var llrpReq llrp.Outgoing
		var llrpResp llrp.Incoming

		switch reqs[i].DeviceResourceName {
		case ResourceReaderConfig:
			llrpReq = &llrp.GetReaderConfig{}
			llrpResp = &llrp.GetReaderConfigResponse{}
		case ResourceReaderCap:
			llrpReq = &llrp.GetReaderCapabilities{}
			llrpResp = &llrp.GetReaderCapabilitiesResponse{}
		case ResourceROSpec:
			llrpReq = &llrp.GetROSpecs{}
			llrpResp = &llrp.GetROSpecsResponse{}
		case ResourceAccessSpec:
			llrpReq = &llrp.GetAccessSpecs{}
			llrpResp = &llrp.GetAccessSpecsResponse{}
		}

		if err := c.SendFor(ctx, llrpReq, llrpResp); err != nil {
			return nil, err
		}

		respData, err := json.Marshal(llrpResp)
		if err != nil {
			return nil, err
		}

		responses[i] = dsModels.NewStringValue(
			reqs[i].DeviceResourceName, time.Now().UnixNano(), string(respData))
	}

	return responses, nil
}

// HandleWriteCommands passes a slice of CommandRequest struct each representing
// a ResourceOperation for a specific device resource.
// Since the commands are actuation commands, params provide parameters for the individual
// command.
func (d *Driver) HandleWriteCommands(devName string, p protocolMap, reqs []dsModels.CommandRequest, params []*dsModels.CommandValue) error {
	d.lc.Debug(fmt.Sprintf("LLRP-Driver.HandleWriteCommands: "+
		"device: %s protocols: %v reqs: %+v", devName, p, reqs))

	if len(reqs) == 0 {
		return errors.New("missing requests")
	}

	c, err := d.getClient(devName, p)
	if err != nil {
		return err
	}

	getParam := func(name string, idx int, key string) (*dsModels.CommandValue, error) {
		if idx > len(params) {
			return nil, errors.Errorf("%s needs at least %d parameters, but got %d",
				name, idx, len(params))
		}

		cv := params[idx]
		if cv == nil {
			return nil, errors.Errorf("%s requires parameter %s", name, key)
		}

		if cv.DeviceResourceName != key {
			return nil, errors.Errorf("%s expected parameter %d: %s, but got %s",
				name, idx, key, cv.DeviceResourceName)
		}

		return cv, nil
	}

	getStrParam := func(name string, idx int, key string) (string, error) {
		if cv, err := getParam(name, idx, key); err != nil {
			return "", err
		} else {
			return cv.StringValue()
		}
	}

	getUint32Param := func(name string, idx int, key string) (uint32, error) {
		if cv, err := getParam(name, idx, key); err != nil {
			return 0, err
		} else {
			return cv.Uint32Value()
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	var llrpReq llrp.Outgoing  // the message to send
	var llrpResp llrp.Incoming // the expected response
	var reqData []byte         // incoming JSON request data, if present
	var dataTarget interface{} // used if the reqData in a subfield of the llrpReq

	switch reqs[0].DeviceResourceName {
	case ResourceReaderConfig:
		data, err := getStrParam("Set"+ResourceReaderConfig, 0, ResourceReaderConfig)
		if err != nil {
			return err
		}

		reqData = []byte(data)
		llrpReq = &llrp.SetReaderConfig{}
		llrpResp = &llrp.SetReaderConfigResponse{}
	case ResourceROSpec:
		data, err := getStrParam("Add"+ResourceROSpec, 0, ResourceROSpec)
		if err != nil {
			return err
		}

		reqData = []byte(data)

		addSpec := llrp.AddROSpec{}
		dataTarget = &addSpec.ROSpec // the incoming data is an ROSpec, not AddROSpec
		llrpReq = &addSpec           // but we want to send AddROSpec, not just ROSpec
		llrpResp = &llrp.AddROSpecResponse{}
	case ResourceROSpecID:
		if len(params) != 2 {
			return errors.Errorf("expected 2 resources for ROSpecID op, but got %d", len(params))
		}

		action, err := getStrParam(ResourceROSpec, 1, ResourceAction)
		if err != nil {
			return err
		}

		roID, err := getUint32Param(action+ResourceROSpec, 0, ResourceROSpecID)
		if err != nil {
			return err
		}

		switch action {
		default:
			return errors.Errorf("unknown ROSpecID action: %q", action)
		case ActionEnable:
			llrpReq = &llrp.EnableROSpec{ROSpecID: roID}
			llrpResp = &llrp.EnableROSpecResponse{}
		case ActionStart:
			llrpReq = &llrp.StartROSpec{ROSpecID: roID}
			llrpResp = &llrp.StartROSpecResponse{}
		case ActionStop:
			llrpReq = &llrp.StopROSpec{ROSpecID: roID}
			llrpResp = &llrp.StopROSpecResponse{}
		case ActionDisable:
			llrpReq = &llrp.DisableROSpec{ROSpecID: roID}
			llrpResp = &llrp.DisableROSpecResponse{}
		case ActionDelete:
			llrpReq = &llrp.DeleteROSpec{ROSpecID: roID}
			llrpResp = &llrp.DeleteROSpecResponse{}
		}

	case ResourceAccessSpecID:
		if len(reqs) != 2 {
			return errors.Errorf("expected 2 resources for AccessSpecID op, but got %d", len(reqs))
		}

		action := reqs[1].DeviceResourceName

		asID, err := getUint32Param(action+ResourceAccessSpecID, 0, ResourceAccessSpecID)
		if err != nil {
			return err
		}

		switch action {
		default:
			return errors.Errorf("unknown ROSpecID action: %q", action)
		case ActionEnable:
			llrpReq = &llrp.EnableAccessSpec{AccessSpecID: asID}
			llrpResp = &llrp.EnableAccessSpecResponse{}
		case ActionDisable:
			llrpReq = &llrp.DisableAccessSpec{AccessSpecID: asID}
			llrpResp = &llrp.DisableAccessSpecResponse{}
		case ActionDelete:
			llrpReq = &llrp.DeleteAccessSpec{AccessSpecID: asID}
			llrpResp = &llrp.DeleteAccessSpecResponse{}
		}
	}

	if reqData != nil {
		if dataTarget != nil {
			if err := json.Unmarshal(reqData, dataTarget); err != nil {
				return errors.Wrap(err, "failed to unmarshal request")
			}
		} else {
			if err := json.Unmarshal(reqData, llrpReq); err != nil {
				return errors.Wrap(err, "failed to unmarshal request")
			}
		}
	}

	// SendFor will handle turning ErrorMessages and failing LLRPStatuses into errors.
	if err := c.SendFor(ctx, llrpReq, llrpResp); err != nil {
		return err
	}

	go func(resName, devName string, resp llrp.Incoming) {
		respData, err := json.Marshal(resp)
		if err != nil {
			d.lc.Error("failed to marshal response", "message", resName, "error", err)
			return
		}

		cv := dsModels.NewStringValue(resName, time.Now().UnixNano(), string(respData))
		d.asyncCh <- &dsModels.AsyncValues{
			DeviceName:    devName,
			CommandValues: []*dsModels.CommandValue{cv},
		}
	}(reqs[0].DeviceResourceName, c.Name, llrpResp)

	return nil
}

// Stop the protocol-specific DS code to shutdown gracefully, or
// if the force parameter is 'true', immediately. The driver is responsible
// for closing any in-use channels, including the channel used to send async
// readings (if supported).
func (d *Driver) Stop(force bool) error {
	// Then Logging Client might not be initialized
	if d.lc == nil {
		d.lc = logger.NewClientStdOut(ServiceName, false, "DEBUG")
		d.lc.Error("EdgeX called Stop without calling Initialize >:(")
	}
	d.lc.Debug("LLRP-Driver.Stop called", "force", force)

	d.clientsMapMu.Lock()
	defer d.clientsMapMu.Unlock()

	var wg *sync.WaitGroup
	if !force {
		wg = new(sync.WaitGroup)
		wg.Add(len(d.clients))
		defer wg.Wait()
	}

	for _, c := range d.clients {
		go func(c *llrp.Client) {
			d.stopClient(c, force)
			if !force {
				wg.Done()
			}
		}(c)
	}

	d.clients = make(map[string]*llrp.Client)
	return nil
}

// AddDevice is a callback function that is invoked
// when a new Device associated with this Device Service is added
func (d *Driver) AddDevice(deviceName string, protocols protocolMap, adminState contract.AdminState) error {
	d.lc.Debug(fmt.Sprintf("Adding new device: %s protocols: %v adminState: %v",
		deviceName, protocols, adminState))
	_, err := d.getClient(deviceName, protocols)
	return err
}

// UpdateDevice is a callback function that is invoked
// when a Device associated with this Device Service is updated
func (d *Driver) UpdateDevice(deviceName string, protocols protocolMap, adminState contract.AdminState) error {
	d.lc.Debug(fmt.Sprintf("Updating device: %s protocols: %v adminState: %v",
		deviceName, protocols, adminState))
	return nil
}

// RemoveDevice is a callback function that is invoked
// when a Device associated with this Device Service is removed
func (d *Driver) RemoveDevice(deviceName string, p protocolMap) error {
	d.lc.Debug(fmt.Sprintf("Removing device: %s protocols: %v", deviceName, p))
	d.removeClient(deviceName, false)
	return nil
}

// handleAsyncMessages forwards JSON-marshaled messages to EdgeX.
//
// Note that the message types that end up here depend on the subscriptions
// when the Client is created, so if you want to add another,
// you'll need to wire up the handler in the getClient code.
func (d *Driver) handleAsyncMessages(c *llrp.Client, msg llrp.Message) {
	var resourceName string
	var event encoding.BinaryUnmarshaler

	switch msg.Type() {
	default:
		return
	case llrp.MsgReaderEventNotification:
		resourceName = ResourceReaderNotification
		event = &llrp.ReaderEventNotification{}
	case llrp.MsgROAccessReport:
		resourceName = ResourceROAccessReport
		event = &llrp.ROAccessReport{}
	}

	if err := msg.UnmarshalTo(event); err != nil {
		d.lc.Error("failed to unmarshal async event from LLRP", "error", err.Error())
		return
	}

	data, err := json.Marshal(event)
	if err != nil {
		d.lc.Error("failed to marshal async event to JSON", "error", err.Error())
		return
	}

	cv := dsModels.NewStringValue(resourceName, time.Now().UnixNano(), string(data))

	d.asyncCh <- &dsModels.AsyncValues{
		DeviceName:    c.Name,
		CommandValues: []*dsModels.CommandValue{cv},
	}
}

// getOrCreate returns a Client, creating one if needed.
//
// If a Client with this name already exists, it returns it.
// Otherwise, calls the createNew function to get a new Client,
// which it adds to the map and then returns.
func (d *Driver) getClient(name string, p protocolMap) (*llrp.Client, error) {
	// Try with just a read lock.
	d.clientsMapMu.RLock()
	c, ok := d.clients[name]
	d.clientsMapMu.RUnlock()
	if ok {
		return c, nil
	}

	addr, err := getAddr(p)
	if err != nil {
		return nil, err
	}
}

func (d *Driver) createClient(name string, addr net.Addr) (*llrp.Client, error) {
	// It's important it holds the lock while creating a new Client.
	// If two requests arrive at about the same time and target the same device,
	// one will block waiting for the lock and the other will create and add a Client.
	// If both requests created a new Client,
	// at most only one would succeed in connecting,
	// so we want to only create one Client, add it to the map,
	// and return that Client to all callers requesting it.
	// However,
	// After adding the Client, it unlocks, then attempts to connect
	// (really the connect can happen before unlock, since it happens in a goroutine).
	// Once it unlocks, the other request gains the lock and must recheck the map.
	// It will retrieve the freshly created Client, and thus return it.
	// Both requests will attempt their Send,
	// which will block until the Client connects (or fails to do so),
	// or until they cancel their Send attempt (e.g., timing out).
	d.clientsMapMu.Lock()
	defer d.clientsMapMu.Unlock()
	c, ok := d.clients[name]
	if ok {
		return c, nil
	}

	// At this point, a single request is creating the Client,
	// though others may be blocked waiting to check the clients map.
	// The goal is to create a Client quickly put it in the map, and return it.
	// After returning (read: in a new goroutine), we manage its connection.
	// In the meantime, multiple callers needing a connection to the same reader
	// will get back a valid Client on which they can Send methods,
	// though those Send methods will block until either the Client is connected
	// or the connection fails (in which case they'll correctly see the failure).
	// Requests for other Client connections will be blocked for a short time
	// while the

	tryDial := func() (*llrp.Client, error) {
		conn, err := net.DialTimeout(addr.Network(), addr.String(), time.Second*30)
		if err != nil {
			return nil, err
		}

		toEdgex := llrp.MessageHandlerFunc(d.handleAsyncMessages)

		return llrp.NewClient(conn,
			llrp.WithName(name),
			llrp.WithLogger(&edgexLLRPClientLogger{devName: name, lc: d.lc}),
			llrp.WithMessageHandler(llrp.MsgROAccessReport, toEdgex),
			llrp.WithMessageHandler(llrp.MsgReaderEventNotification, toEdgex),
		)
	}

	c, err = tryDial()
	if err != nil {
		return nil, err
	}

	go func() {
		var c *llrp.Client
		err := retry.Slow.RetrySome(retry.Forever, func() (recoverable bool, err error) {
			c, err = tryDial()
			neterr, ok := err.(net.Error)
			recoverable = ok && neterr.Temporary()
			return
		})

		if err != nil {
		}

		// blocks until the Client is closed
		err = c.Connect()
		d.removeClient(c.Name, false)
		if err == nil || errors.Is(err, llrp.ErrClientClosed) {
			return
		}

		d.lc.Error(err.Error())

		// client closed without call to Close or Shutdown;
		// try to reconnect
		retry.Slow.RetrySome(retry.Forever, func() (recoverable bool, err error) {
			if
		})
	}()

	d.clients[name] = c
	return c, nil
}

// removeClient deletes a Client from the clients map.
func (d *Driver) removeClient(deviceName string, force bool) {
	d.clientsMapMu.Lock()
	defer d.clientsMapMu.Unlock()

	if c, ok := d.clients[deviceName]; ok {
		delete(d.clients, deviceName)
		go d.stopClient(c, force)
	}
}

func (d *Driver) stopClient(c *llrp.Client, force bool) {
	if !force {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := c.Shutdown(ctx)
		if err == nil || errors.Is(err, llrp.ErrClientClosed) {
			return
		}
		d.lc.Error("error attempting graceful client shutdown", "error", err.Error())
	}

	if err := c.Close(); err != nil && !errors.Is(err, llrp.ErrClientClosed) {
		d.lc.Error("error attempting forceful client shutdown", "error", err.Error())
	}
}

// getAddr extracts an address from a protocol mapping.
//
// It expects the map to have {"tcp": {"host": "<ip>", "port": "<port>"}}.
func getAddr(protocols protocolMap) (net.Addr, error) {
	tcpInfo := protocols["tcp"]
	if tcpInfo == nil {
		return nil, errors.New("missing tcp protocol")
	}

	host := tcpInfo["host"]
	port := tcpInfo["port"]
	if host == "" || port == "" {
		return nil, errors.Errorf("tcp missing host or port (%q, %q)", host, port)
	}

	addr, err := net.ResolveTCPAddr("tcp", host+":"+port)
	return addr, errors.Wrapf(err,
		"unable to create addr for tcp protocol (%q, %q)", host, port)
}

func (d *Driver) addProvisionWatcher() error {
	var provisionWatcher contract.ProvisionWatcher
	data, err := ioutil.ReadFile("res/provisionwatcher.json")
	if err != nil {
		d.lc.Error(err.Error())
		return err
	}

	err = provisionWatcher.UnmarshalJSON(data)
	if err != nil {
		d.lc.Error(err.Error())
		return err
	}

	if err := d.service().AddOrUpdateProvisionWatcher(provisionWatcher); err != nil {
		d.lc.Info(err.Error())
		return err
	}

	return nil
}

func (d *Driver) Discover() {
	d.lc.Info("*** Discover was called ***")
	d.deviceCh <- autoDiscover()
	d.lc.Info("scanning complete")
}
