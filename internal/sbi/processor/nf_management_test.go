package processor

import (
	"encoding/json"
	"testing"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/free5gc/openapi/models"
)

func TestValidateNfProfilePatch(t *testing.T) {
	tests := []struct {
		name    string
		patch   string
		wantErr string
	}{
		{
			name:  "ordinary heart-beat",
			patch: `[{"op":"replace","path":"/nfStatus","value":"REGISTERED"}]`,
		},
		{
			name:  "load update",
			patch: `[{"op":"replace","path":"/load","value":42}]`,
		},
		{
			name:    "nfInstanceId is the resource identity",
			patch:   `[{"op":"replace","path":"/nfInstanceId","value":"other"}]`,
			wantErr: "nfInstanceId is immutable",
		},
		{
			name:    "nfInstanceId reached through a subpath",
			patch:   `[{"op":"remove","path":"/nfInstanceId/0"}]`,
			wantErr: "nfInstanceId is immutable",
		},
		{
			// We own the interval (TS 29.510 clause 5.2.2.3), and NFs reset their
			// ticker from the response, so patching it to 0 would self-suspend.
			name:    "heartBeatTimer belongs to the NRF",
			patch:   `[{"op":"replace","path":"/heartBeatTimer","value":0}]`,
			wantErr: "heartBeatTimer is set by the NRF",
		},
		{
			name:    "copy source is checked too",
			patch:   `[{"op":"copy","from":"/nfInstanceId","path":"/fqdn"}]`,
			wantErr: "nfInstanceId is immutable",
		},
		{
			name:    "mis-cased nfInstanceId is still rejected",
			patch:   `[{"op":"replace","path":"/NfInstanceId","value":"other"}]`,
			wantErr: "nfInstanceId is immutable",
		},
		{
			name:    "mis-cased heartBeatTimer is still rejected",
			patch:   `[{"op":"replace","path":"/HeartBeatTimer","value":0}]`,
			wantErr: "heartBeatTimer is set by the NRF",
		},
		{
			name:    "malformed payload",
			patch:   `not json`,
			wantErr: "invalid JSON Patch payload",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateNfProfilePatch([]byte(tt.patch))
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// TestCheckPatchInvariants pins the whole-document escape: a root-pointer op
// (path "", RFC 6901) never matches the path guards in validateNfProfilePatch,
// so the applied result must be checked against the stored profile.
func TestCheckPatchInvariants(t *testing.T) {
	const original = `{"nfInstanceId":"nf-1","nfType":"AUSF","nfStatus":"REGISTERED","heartBeatTimer":10}`

	tests := []struct {
		name    string
		patch   string
		wantErr string
	}{
		{
			name:  "ordinary heart-beat",
			patch: `[{"op":"replace","path":"/nfStatus","value":"REGISTERED"}]`,
		},
		{
			name: "root replace preserving the NRF-owned fields",
			patch: `[{"op":"replace","path":"","value":` +
				`{"nfInstanceId":"nf-1","nfType":"AMF","nfStatus":"UNDISCOVERABLE","heartBeatTimer":10}}]`,
		},
		{
			name: "root replace renaming nfInstanceId",
			patch: `[{"op":"replace","path":"","value":` +
				`{"nfInstanceId":"other","nfType":"AUSF","nfStatus":"REGISTERED","heartBeatTimer":10}}]`,
			wantErr: "nfInstanceId is immutable",
		},
		{
			name: "root replace changing heartBeatTimer",
			patch: `[{"op":"replace","path":"","value":` +
				`{"nfInstanceId":"nf-1","nfType":"AUSF","nfStatus":"REGISTERED","heartBeatTimer":3600}}]`,
			wantErr: "heartBeatTimer is set by the NRF",
		},
		{
			name: "root replace dropping heartBeatTimer",
			patch: `[{"op":"replace","path":"","value":` +
				`{"nfInstanceId":"nf-1","nfType":"AUSF","nfStatus":"REGISTERED"}}]`,
			wantErr: "heartBeatTimer is set by the NRF",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patch, err := jsonpatch.DecodePatch([]byte(tt.patch))
			require.NoError(t, err)
			patchedJSON, applyErr := patch.Apply([]byte(original))
			require.NoError(t, applyErr)

			var originalProfile, patchedProfile models.NrfNfManagementNfProfile
			require.NoError(t, json.Unmarshal([]byte(original), &originalProfile))
			require.NoError(t, json.Unmarshal(patchedJSON, &patchedProfile))

			err = checkPatchInvariants(&originalProfile, &patchedProfile)
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// TestNfStatusPatched pins the byte-exact pointer comparison: "/NfStatus" is
// a different JSON Pointer from "/nfStatus" (RFC 6901), so a patch naming it
// must still count as a plain heart-beat.
func TestNfStatusPatched(t *testing.T) {
	tests := []struct {
		name  string
		patch string
		want  bool
	}{
		{
			name:  "explicit nfStatus is the NF's own choice",
			patch: `[{"op":"replace","path":"/nfStatus","value":"UNDISCOVERABLE"}]`,
			want:  true,
		},
		{
			name:  "whole-document pointer rewrites nfStatus with everything else",
			patch: `[{"op":"replace","path":"","value":{}}]`,
			want:  true,
		},
		{
			name:  "load-only heart-beat",
			patch: `[{"op":"replace","path":"/load","value":42}]`,
			want:  false,
		},
		{
			name:  "mis-cased pointer names a different attribute",
			patch: `[{"op":"add","path":"/NfStatus","value":"REGISTERED"}]`,
			want:  false,
		},
		{
			name:  "test op asserts, never writes",
			patch: `[{"op":"test","path":"/nfStatus","value":"SUSPENDED"}]`,
			want:  false,
		},
		{
			name:  "test-guarded load update is still a pure heart-beat",
			patch: `[{"op":"test","path":"/nfStatus","value":"SUSPENDED"},{"op":"replace","path":"/load","value":42}]`,
			want:  false,
		},
		{
			name:  "malformed patch",
			patch: `not json`,
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, nfStatusPatched([]byte(tt.patch)))
		})
	}
}
