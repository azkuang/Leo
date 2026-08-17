package hardware

import (
	"math"
	"testing"
	"unsafe"

	"github.com/NVIDIA/go-dcgm/pkg/dcgm"
)

func intFieldValue(fieldID dcgm.Short, value int64) dcgm.FieldValue_v2 {
	v := dcgm.FieldValue_v2{FieldID: fieldID, FieldType: dcgm.DCGM_FT_INT64}
	*(*int64)(unsafe.Pointer(&v.Value[0])) = value
	return v
}

func doubleFieldValue(fieldID dcgm.Short, value float64) dcgm.FieldValue_v2 {
	v := dcgm.FieldValue_v2{FieldID: fieldID, FieldType: dcgm.DCGM_FT_DOUBLE}
	*(*float64)(unsafe.Pointer(&v.Value[0])) = value
	return v
}

func TestDecodeFieldValues(t *testing.T) {
	values := []dcgm.FieldValue_v2{
		intFieldValue(dcgm.DCGM_FI_DEV_GPU_UTIL, 42),  // 42% -> 0.42
		intFieldValue(dcgm.DCGM_FI_DEV_FB_FREE, 8192), // MB -> GB
		intFieldValue(dcgm.DCGM_FI_DEV_GPU_TEMP, 71),
		doubleFieldValue(dcgm.DCGM_FI_DEV_POWER_USAGE, 250.5),
		intFieldValue(dcgm.DCGM_FI_DEV_CLOCK_THROTTLE_REASONS, int64(throttleHWThermalSlowdown)),
		intFieldValue(dcgm.DCGM_FI_DEV_XID_ERROR, 79),
		intFieldValue(dcgm.DCGM_FI_DEV_ROW_REMAP_UNCORRECTABLE_TOTAL, 3),
		intFieldValue(dcgm.DCGM_FI_DEV_PCIE_REPLAY_COUNTER, 5),
		intFieldValue(dcgm.DCGM_FI_DEV_ECC_DBE_VOL_TOTAL, 1),
	}

	d := decodeFieldValues(values)

	if d.utilization != 0.42 {
		t.Errorf("utilization = %v, want 0.42", d.utilization)
	}
	if d.freeVRAMGB != 8 {
		t.Errorf("freeVRAMGB = %v, want 8", d.freeVRAMGB)
	}
	if d.temperatureC != 71 {
		t.Errorf("temperatureC = %v, want 71", d.temperatureC)
	}
	if d.powerW != 250.5 {
		t.Errorf("powerW = %v, want 250.5", d.powerW)
	}
	if d.throttleBits != throttleHWThermalSlowdown {
		t.Errorf("throttleBits = %v, want %v", d.throttleBits, throttleHWThermalSlowdown)
	}
	if d.xid != 79 {
		t.Errorf("xid = %v, want 79", d.xid)
	}
	if d.rowRemapPending != 3 {
		t.Errorf("rowRemapPending = %v, want 3", d.rowRemapPending)
	}
	if d.pcieReplayCount != 5 {
		t.Errorf("pcieReplayCount = %v, want 5", d.pcieReplayCount)
	}
	if d.eccVolatileErrors != 1 {
		t.Errorf("eccVolatileErrors = %v, want 1", d.eccVolatileErrors)
	}
}

// TestDecodeFieldValuesBlankIsUnsupported is the regression test for the bug
// this replaces: DCGM's blank sentinel must be treated as "unsupported by
// this card" (left at zero), never surfaced as a huge fake reading -- and,
// separately, must not be confused with a real zero-valued sample.
func TestDecodeFieldValuesBlankIsUnsupported(t *testing.T) {
	values := []dcgm.FieldValue_v2{
		intFieldValue(dcgm.DCGM_FI_DEV_PCIE_REPLAY_COUNTER, dcgm.DCGM_FT_INT64_BLANK),
		intFieldValue(dcgm.DCGM_FI_DEV_ECC_DBE_VOL_TOTAL, dcgm.DCGM_FT_INT64_NOT_SUPPORTED),
		doubleFieldValue(dcgm.DCGM_FI_DEV_POWER_USAGE, dcgm.DCGM_FT_FP64_BLANK),
	}

	d := decodeFieldValues(values)

	if d.pcieReplayCount != 0 {
		t.Errorf("blank PCIe replay count should decode to 0, got %d", d.pcieReplayCount)
	}
	if d.eccVolatileErrors != 0 {
		t.Errorf("blank ECC error count should decode to 0, got %d", d.eccVolatileErrors)
	}
	if d.powerW != 0 {
		t.Errorf("blank power reading should decode to 0, got %v", d.powerW)
	}
}

func TestDecodeThrottleReasonsNone(t *testing.T) {
	got := decodeThrottleReasons(0)
	if len(got) != 1 || got[0] != "none" {
		t.Errorf("decodeThrottleReasons(0) = %v, want [none]", got)
	}
}

func TestDecodeThrottleReasonsMultiple(t *testing.T) {
	got := decodeThrottleReasons(throttleHWThermalSlowdown | throttleSWPowerCap)
	if len(got) != 2 {
		t.Fatalf("expected 2 reasons, got %v", got)
	}
}

func TestFieldValueEncodingRoundTrip(t *testing.T) {
	// Sanity check that this test file's helpers encode values exactly the
	// way dcgm.FieldValue_v2.Int64()/Float64() decode them, since that is an
	// unsafe-pointer cast this package does not control.
	v := intFieldValue(dcgm.DCGM_FI_DEV_GPU_TEMP, 12345)
	if v.Int64() != 12345 {
		t.Fatalf("encoding helper does not round-trip through Int64(): got %d", v.Int64())
	}
	fv := doubleFieldValue(dcgm.DCGM_FI_DEV_POWER_USAGE, math.Pi)
	if fv.Float64() != math.Pi {
		t.Fatalf("encoding helper does not round-trip through Float64(): got %v", fv.Float64())
	}
}
