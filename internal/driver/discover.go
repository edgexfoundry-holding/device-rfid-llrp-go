//
// Copyright (C) 2020 Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/edgexfoundry/device-rfid-llrp-go/internal/llrp"
	dsModels "github.com/edgexfoundry/device-sdk-go/pkg/models"
	contract "github.com/edgexfoundry/go-mod-core-contracts/models"
	"github.com/pkg/errors"
	"math"
	"math/bits"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultDevicePrefix    = "LLRP"
	unknownVendorID        = 0
	unknownModelID         = 0
	unknownFirmwareVersion = ""
)

// discoveryInfo holds information about a discovered device
type discoveryInfo struct {
	deviceName string
	host       string
	port       string
	vendor     uint32
	model      uint32
	fwVersion  string
}

// newDiscoveryInfo creates a new discoveryInfo with just a host and port pre-filled
func newDiscoveryInfo(host, port string) *discoveryInfo {
	return &discoveryInfo{
		host: host,
		port: port,
	}
}

// workerParams is a helper struct to store shared parameters to ipWorkers
type workerParams struct {
	deviceMap map[string]contract.Device
	ipCh      <-chan uint32
	resultCh  chan<- *discoveryInfo
	ctx       context.Context

	timeout  time.Duration
	scanPort string
}

type discoverParams struct {
	subnets    []string
	asyncLimit int
	timeout    time.Duration
	scanPort   string
}

// computeNetSz computes the total amount of valid IP addresses for a given subnet size
// Subnets of size 31 and 32 have only 1 valid IP address
// Ex. For a /24 subnet, computeNetSz(24) -> 254
func computeNetSz(subnetSz int) uint32 {
	if subnetSz >= 31 {
		return 1
	}
	return ^uint32(0)>>subnetSz - 1
}

// autoDiscover probes all addresses in the configured network to attempt to discover any possible
// RFID readers that support LLRP.
func autoDiscover(ctx context.Context, params discoverParams) []dsModels.DiscoveredDevice {
	if len(params.subnets) == 0 {
		driver.lc.Warn("Discover was called, but no subnet information has been configured!")
		return nil
	}

	ipnets := make([]*net.IPNet, 0, len(params.subnets))
	var estimatedProbes int
	for _, cidr := range params.subnets {
		if cidr == "" {
			driver.lc.Warn("Empty CIDR provided, unable to scan for LLRP readers.")
			continue
		}

		ip, ipnet, err := net.ParseCIDR(cidr)
		if err != nil {
			driver.lc.Error(fmt.Sprintf("Unable to parse CIDR: %q", cidr), "error", err)
			continue
		}
		if ip == nil || ipnet == nil || ip.To4() == nil {
			driver.lc.Error("Currently only ipv4 subnets are supported.", "subnet", cidr)
			continue
		}

		ipnets = append(ipnets, ipnet)
		// compute the estimate total amount of network probes we are going to make
		// this is an estimate because it may be lower due to skipped addresses (existing devices)
		sz, _ := ipnet.Mask.Size()
		estimatedProbes += int(computeNetSz(sz))
	}

	// if the estimated amount of probes we are going to make is less than
	// the async limit, we only need to set the worker count to the total number
	// of probes to avoid spawning more workers than probes
	asyncLimit := params.asyncLimit
	if estimatedProbes < asyncLimit {
		asyncLimit = estimatedProbes
	}
	driver.lc.Debug(fmt.Sprintf("total estimated network probes: %d, async limit: %d, probe timeout: %v, total estimated time: %v",
		estimatedProbes, asyncLimit, params.timeout, time.Duration(math.Ceil(float64(estimatedProbes)/float64(asyncLimit)))*params.timeout))

	ipCh := make(chan uint32, asyncLimit)
	resultCh := make(chan *discoveryInfo)

	deviceMap := makeDeviceMap()
	wParams := workerParams{
		deviceMap: deviceMap,
		ipCh:      ipCh,
		resultCh:  resultCh,
		ctx:       ctx,
		timeout:   params.timeout,
		scanPort:  params.scanPort,
	}

	// start the workers before adding any ips so they are ready to process
	var wgIPWorkers sync.WaitGroup
	wgIPWorkers.Add(asyncLimit)
	for i := 0; i < asyncLimit; i++ {
		go func() {
			defer wgIPWorkers.Done()
			ipWorker(wParams)
		}()
	}

	go func() {
		var wgIPGenerators sync.WaitGroup
		for _, ipnet := range ipnets {
			select {
			case <-ctx.Done():
				// quit early if we have been cancelled
				return
			default:
			}

			// wait on each ipGenerator
			wgIPGenerators.Add(1)
			go func(inet *net.IPNet) {
				defer wgIPGenerators.Done()
				ipGenerator(ctx, inet, ipCh)
			}(ipnet)
		}

		// wait for all ip generators to finish, then we can close the ip channel
		wgIPGenerators.Wait()
		close(ipCh)

		// wait for the ipWorkers to finish, then close the results channel which
		// will let the enclosing function finish
		wgIPWorkers.Wait()
		close(resultCh)
	}()

	// this blocks until the resultCh is closed in above go routine
	return processResultChannel(resultCh, deviceMap)
}

// processResultChannel reads all incoming results until the resultCh is closed.
// it determines if a device is new or existing, and proceeds accordingly.
//
// Does not check for context cancellation because we still want to
// process any in-flight results.
func processResultChannel(resultCh chan *discoveryInfo, deviceMap map[string]contract.Device) []dsModels.DiscoveredDevice {
	discovered := make([]dsModels.DiscoveredDevice, 0)
	for info := range resultCh {
		if info == nil {
			continue
		}

		// check if any devices already exist at that address, and if so disable them
		existing, found := deviceMap[info.host+":"+info.port]
		if found && existing.Name != info.deviceName {
			// disable it and remove its protocol information since it is no longer valid
			delete(existing.Protocols, "tcp")
			existing.OperatingState = contract.Disabled
			if err := driver.svc.UpdateDevice(existing); err != nil {
				driver.lc.Warn("There was an issue trying to disable an existing device.",
					"deviceName", existing.Name,
					"error", err)
			}
		}

		// check if we have an existing device registered with this name
		device, err := driver.svc.GetDeviceByName(info.deviceName)
		if err != nil {
			// no existing device; add it to the list and move on
			discovered = append(discovered, newDiscoveredDevice(info))
			continue
		}

		// this means we have discovered an existing device that is
		// either disabled or has changed IP addresses.
		// we need to update its protocol information and operating state
		if err := info.updateExistingDevice(device); err != nil {
			driver.lc.Warn("There was an issue trying to update an existing device based on newly discovered details.",
				"deviceName", device.Name,
				"discoveryInfo", fmt.Sprintf("%+v", info),
				"error", err)
		}
	}
	return discovered
}

// updateExistingDevice is used when an existing device is discovered
// and needs to update its information to either a new address or set
// its operating state to enabled.
func (info *discoveryInfo) updateExistingDevice(device contract.Device) error {
	shouldUpdate := false
	if device.OperatingState == contract.Disabled {
		device.OperatingState = contract.Enabled
		shouldUpdate = true
	}

	tcpInfo := device.Protocols["tcp"]
	if tcpInfo == nil ||
		info.host != tcpInfo["host"] ||
		info.port != tcpInfo["port"] {
		driver.lc.Info("Existing device has been discovered with a different network address.",
			"oldInfo", fmt.Sprintf("%+v", tcpInfo),
			"discoveredInfo", fmt.Sprintf("%+v", info))

		device.Protocols["tcp"] = map[string]string{
			"host": info.host,
			"port": info.port,
		}
		// make sure it is enabled
		device.OperatingState = contract.Enabled
		shouldUpdate = true
	}

	if !shouldUpdate {
		// the address is the same and device is already enabled, should not reach here
		driver.lc.Warn("Re-discovered existing device at the same TCP address, nothing to do.")
		return nil
	}

	if err := driver.svc.UpdateDevice(device); err != nil {
		driver.lc.Error("There was an error updating the tcp address for an existing device.",
			"deviceName", device.Name,
			"error", err)
		return err
	}

	return nil
}

// makeDeviceMap creates a lookup table of existing devices by tcp address in order to skip scanning
func makeDeviceMap() map[string]contract.Device {
	devices := driver.svc.Devices()
	deviceMap := make(map[string]contract.Device, len(devices))

	for _, d := range devices {
		tcpInfo := d.Protocols["tcp"]
		if tcpInfo == nil {
			driver.lc.Warn("Found registered device without tcp protocol information.", "deviceName", d.Name)
			continue
		}

		host, port := tcpInfo["host"], tcpInfo["port"]
		if host == "" || port == "" {
			driver.lc.Warn("Registered device is missing required tcp protocol information.",
				"host", host,
				"port", port)
			continue
		}

		deviceMap[host+":"+port] = d
	}

	return deviceMap
}

// ipGenerator generates all valid IP addresses for a given subnet, and
// sends them to the ip channel one at a time
func ipGenerator(ctx context.Context, inet *net.IPNet, ipCh chan<- uint32) {
	addr := inet.IP.To4()
	if addr == nil {
		return
	}

	mask := inet.Mask
	if len(mask) == net.IPv6len {
		mask = mask[12:]
	} else if len(mask) != net.IPv4len {
		return
	}

	umask := binary.BigEndian.Uint32(mask)
	maskSz := bits.OnesCount32(umask)
	if maskSz <= 1 {
		return // skip point-to-point connections
	} else if maskSz >= 31 {
		ipCh <- binary.BigEndian.Uint32(inet.IP)
		return
	}

	netId := binary.BigEndian.Uint32(addr) & umask // network ID
	bcast := netId ^ (^umask)
	for ip := netId + 1; ip < bcast; ip++ {
		if netId&umask != ip&umask {
			continue
		}

		select {
		case <-ctx.Done():
			// bail if we have been cancelled
			return
		case ipCh <- ip:
		}
	}
}

// probe attempts to make a connection to a specific ip and port to determine
// if an LLRP reader exists at that network address
func probe(host, port string, timeout time.Duration) (*discoveryInfo, error) {
	addr := host + ":" + port
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	driver.lc.Info("Connection dialed", "host", host, "port", port)
	c := llrp.NewClient(llrp.WithLogger(&edgexLLRPClientLogger{
		devName: "probe-" + host,
		lc:      driver.lc,
	}))

	readerConfig := llrp.GetReaderConfigResponse{}
	readerCaps := llrp.GetReaderCapabilitiesResponse{}

	// send llrp messages in a separate thread and block the main thread until it is complete
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), sendTimeout)
		defer cancel()

		defer func() {
			if err := c.Shutdown(ctx); err != nil && !errors.Is(err, llrp.ErrClientClosed) {
				driver.lc.Warn("Error closing discovery device.", "error", err.Error())
				_ = c.Close()
			}
		}()

		driver.lc.Debug("Sending GetReaderConfig.")
		configReq := llrp.GetReaderConfig{
			RequestedData: llrp.ReaderConfReqIdentification,
		}
		err = c.SendFor(ctx, &configReq, &readerConfig)
		if errors.Is(err, llrp.ErrClientClosed) {
			driver.lc.Warn("Client connection was closed while sending GetReaderConfig to discovered device.", "error", err)
			return
		} else if err != nil {
			driver.lc.Warn("Error sending GetReaderConfig to discovered device.", "error", err)
			return
		}

		driver.lc.Debug("Sending GetReaderCapabilities.")
		capabilitiesReq := llrp.GetReaderCapabilities{
			ReaderCapabilitiesRequestedData: llrp.ReaderCapGeneralDeviceCapabilities,
		}
		err = c.SendFor(ctx, &capabilitiesReq, &readerCaps)
		if errors.Is(err, llrp.ErrClientClosed) {
			driver.lc.Warn("Client connection was closed while sending GetReaderCapabilities to discovered device.", "error", err)
			return
		} else if err != nil {
			driver.lc.Warn("Error sending GetReaderCapabilities to discovered device.", "error", err)
			return
		}
	}()

	driver.lc.Info("Attempting to connect to potential LLRP device...", "host", host, "port", port)
	// this will block until `c.Shutdown()` is called in the above go routine
	if err = c.Connect(conn); !errors.Is(err, llrp.ErrClientClosed) {
		driver.lc.Warn("Error attempting to connect to potential LLRP device: " + err.Error())
		return nil, err
	}
	driver.lc.Info("Connection initiated successfully.", "host", host, "port", port)

	info := newDiscoveryInfo(host, port)
	if readerCaps.GeneralDeviceCapabilities == nil {
		driver.lc.Warn("ReaderCapabilities.GeneralDeviceCapabilities was nil, unable to determine vendor and model info")
		info.vendor = unknownVendorID
		info.model = unknownModelID
		info.fwVersion = unknownFirmwareVersion
	} else {
		info.vendor = readerCaps.GeneralDeviceCapabilities.DeviceManufacturer
		info.model = readerCaps.GeneralDeviceCapabilities.Model
		info.fwVersion = readerCaps.GeneralDeviceCapabilities.FirmwareVersion
	}

	if readerConfig.Identification == nil {
		driver.lc.Warn("ReaderConfig.Identification was nil, unable to register discovered device.")
		return nil, fmt.Errorf("unable to retrieve device identification")
	}

	prefix := defaultDevicePrefix
	if VendorIDType(info.vendor) == Impinj {
		prefix = ImpinjModelType(info.model).HostnamePrefix()
	}

	var suffix string
	rID := readerConfig.Identification.ReaderID
	if readerConfig.Identification.IDType == llrp.ID_MAC_EUI64 && len(rID) >= 3 {
		mac := rID[len(rID)-3:]
		suffix = fmt.Sprintf("%02X-%02X-%02X", mac[0], mac[1], mac[2])
	} else {
		suffix = hex.EncodeToString(rID)
	}

	info.deviceName = prefix + "-" + suffix
	driver.lc.Info(fmt.Sprintf("Discovered device: %+v", info))

	return info, nil
}

// ipWorker pulls uint32s, convert to IPs, and sends back successful probes to the resultCh
func ipWorker(params workerParams) {
	ip := net.IP([]byte{0, 0, 0, 0})

	for {
		select {
		case <-params.ctx.Done():
			// stop working if we have been cancelled
			return

		case a, ok := <-params.ipCh:
			if !ok {
				// channel has been closed
				return
			}

			binary.BigEndian.PutUint32(ip, a)

			ipStr := ip.String()
			addr := ipStr + ":" + params.scanPort
			if d, found := params.deviceMap[addr]; found {
				if d.OperatingState == contract.Enabled {
					driver.lc.Debug("Skip scan of " + addr + ", device already registered.")
					continue
				}
				driver.lc.Info("Existing device in disabled (disconnected) state will be scanned again.",
					"address", addr,
					"deviceName", d.Name)
			}

			select {
			case <-params.ctx.Done():
				// bail if we have already been cancelled
				return
			default:
			}

			if info, err := probe(ipStr, params.scanPort, params.timeout); err == nil && info != nil {
				params.resultCh <- info
			}
		}
	}
}

// newDiscoveredDevice takes the host and port number of a discovered LLRP reader and prepares it for
// registration with EdgeX
func newDiscoveredDevice(info *discoveryInfo) dsModels.DiscoveredDevice {
	labels := []string{
		"RFID",
		"LLRP",
	}

	// Add vendor string only if it is a known vendor
	vendorStr := VendorIDType(info.vendor).String()
	if !strings.HasPrefix(vendorStr, "VendorIDType(") {
		labels = append(labels, vendorStr)
	} else {
		vendorStr = ""
	}

	// Add model string if it is available. Currently only supported for a select few Impinj devices.
	var modelStr string
	if VendorIDType(info.vendor) == Impinj {
		ipjModelStr := ImpinjModelType(info.model).String()
		if !strings.HasPrefix(modelStr, "ImpinjModelType(") {
			labels = append(labels, modelStr)
			modelStr = ipjModelStr
		}
	}

	// Note that we are adding vendorPEN to the protocol properties in order to
	// allow the provision watchers to be able to match against that info. Currently
	// that is the only thing the provision watchers use for matching against.
	return dsModels.DiscoveredDevice{
		Name: info.deviceName,
		Protocols: map[string]contract.ProtocolProperties{
			"tcp": {
				"host": info.host,
				"port": info.port,
			},
			// we are adding a "metadata" protocol as a place to store general properties for
			// matching against provision watchers. It is not an actual protocol, but
			// the recommended way of adding generic key/value pairs to a device.
			"metadata": {
				"vendorPEN": strconv.FormatUint(uint64(info.vendor), 10),
				"vendorStr": vendorStr,
				"model":     strconv.FormatUint(uint64(info.model), 10),
				"modelStr":  modelStr,
				"fwVersion": info.fwVersion,
			},
		},
		Description: "LLRP RFID Reader",
		Labels:      labels,
	}
}
