package factory

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/free5gc/openapi/models"
)

func TestHeartbeatDefaults(t *testing.T) {
	tests := []struct {
		name              string
		heartbeat         *Heartbeat
		wantTimer         int
		wantSuspendFactor int
		wantDropDelay     int
	}{
		// A dropDelay of 0 means deregistration is disabled: unlike the other
		// two fields it has no default, the drop sweep is opt-in.
		{
			"absent block", nil,
			NrfDefaultHeartbeatTimer, NrfDefaultHeartbeatSuspendFactor, 0,
		},
		{
			"empty block", &Heartbeat{},
			NrfDefaultHeartbeatTimer, NrfDefaultHeartbeatSuspendFactor, 0,
		},
		{
			"configured", &Heartbeat{Timer: 30, SuspendFactor: 3, DropDelay: 7200},
			30, 3, 7200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{Configuration: &Configuration{Heartbeat: tt.heartbeat}}
			assert.Equal(t, tt.wantTimer, cfg.GetHeartbeatTimer())
			assert.Equal(t, tt.wantSuspendFactor, cfg.GetHeartbeatSuspendFactor())
			assert.Equal(t, tt.wantDropDelay, cfg.GetHeartbeatDropDelay())
		})
	}
}

// A heartbeat timer above 3600 must fail at config load: the profile
// validator caps heartBeatTimer at 3600 and runs on the config-derived value
// at every registration.
func TestValidateHeartbeatRange(t *testing.T) {
	tests := []struct {
		name      string
		heartbeat *Heartbeat
		wantErr   bool
	}{
		{"absent block", nil, false},
		{"configured in range", &Heartbeat{Timer: 60, SuspendFactor: 3}, false},
		{"timer at profile validator cap", &Heartbeat{Timer: 3600, DropDelay: 14400}, false},
		{"timer above profile validator cap", &Heartbeat{Timer: 7200}, true},
		{"suspend factor below lower bound", &Heartbeat{SuspendFactor: 1}, true},
		{"suspend factor at upper bound", &Heartbeat{SuspendFactor: 10}, false},
		{"suspend factor above upper bound", &Heartbeat{SuspendFactor: 11}, true},
		{"drop delay in range", &Heartbeat{DropDelay: 7200}, false},
		{"drop delay below range floor", &Heartbeat{DropDelay: 59}, true},
		{"drop delay above range ceiling", &Heartbeat{DropDelay: 604801}, true},
		// A dropDelay at or below the suspension deadline must fail at load, or
		// instances would be deregistered the moment they are suspended.
		{"drop delay at suspension deadline", &Heartbeat{Timer: 100, SuspendFactor: 5, DropDelay: 500}, true},
		{"drop delay above suspension deadline", &Heartbeat{Timer: 100, SuspendFactor: 5, DropDelay: 501}, false},
		// With dropDelay unset the drop sweep is off, so the cross-field check
		// does not apply and any valid timer stands alone.
		{"large timer with drop disabled", &Heartbeat{Timer: 3600}, false},
		// The dropDelay range ceiling (604800) must leave valid choices above the
		// largest possible suspension deadline (3600 * 10), or the extreme corner
		// of the timer and suspendFactor ranges would be unsatisfiable.
		{
			"max deadline satisfiable within dropDelay range",
			&Heartbeat{Timer: 3600, SuspendFactor: 10, DropDelay: 604800}, false,
		},
		{
			"max deadline rejects in-range dropDelay below it",
			&Heartbeat{Timer: 3600, SuspendFactor: 10, DropDelay: 36000}, true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				Info: &Info{Version: "1.0.2"},
				Configuration: &Configuration{
					Sbi:             &Sbi{Scheme: "http", BindingIPv4: "127.0.0.1"},
					MongoDBName:     "free5gc",
					MongoDBUrl:      "mongodb://127.0.0.1:27017",
					Heartbeat:       tt.heartbeat,
					DefaultPlmnId:   models.PlmnId{Mcc: "208", Mnc: "93"},
					ServiceNameList: []string{"nnrf-nfm", "nnrf-disc"},
				},
				Logger: &Logger{Level: "info"},
			}
			_, err := cfg.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
