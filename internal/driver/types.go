//
// Copyright (C) 2020 Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

//go:generate stringer -type=VendorIDType,ImpinjModelType -output types_string.go

package driver

type VendorIDType uint32

const (
	Impinj = VendorIDType(25882)
	Alien  = VendorIDType(17996)
	Zebra  = VendorIDType(10642)
)

type ImpinjModelType uint32

const (
	SpeedwayR220 = ImpinjModelType(2001001)
	SpeedwayR420 = ImpinjModelType(2001002)
	XPortal      = ImpinjModelType(2001003)
	XArrayWM     = ImpinjModelType(2001004)
	XArrayEAP    = ImpinjModelType(2001006)
	XArray       = ImpinjModelType(2001007)
	XSpan        = ImpinjModelType(2001008)
	SpeedwayR120 = ImpinjModelType(2001009)
	R700         = ImpinjModelType(2001052)
)

// HostnamePrefix will return the default hostname prefix of known Impinj readers
func (imt ImpinjModelType) HostnamePrefix() string {
	switch imt {
	case SpeedwayR120, SpeedwayR220, SpeedwayR420, R700, XPortal:
		return "SpeedwayR"
	case XSpan:
		return "xSpan"
	case XArray, XArrayEAP, XArrayWM:
		return "xArray"
	default:
		return defaultDevicePrefix
	}
}
