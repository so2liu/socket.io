package engine

import (
	"testing"

	"github.com/zishang520/socket.io/servers/engine/v3/transports"
	"github.com/zishang520/socket.io/v3/pkg/types"
)

func TestOrderedTransportCtors(t *testing.T) {
	transportSet := types.NewSet[TransportCtor](&WebTransportBuilder{}, &WebSocketBuilder{}, &PollingBuilder{})

	for range 100 {
		got := orderedTransportCtors(transportSet)
		gotNames := make([]string, 0, len(got))
		for _, transport := range got {
			gotNames = append(gotNames, transport.Name())
		}

		want := []string{transports.POLLING, transports.WEBSOCKET, transports.WEBTRANSPORT}
		if len(gotNames) != len(want) {
			t.Fatalf("len = %d, want %d (%v)", len(gotNames), len(want), gotNames)
		}
		for i := range want {
			if gotNames[i] != want[i] {
				t.Fatalf("transport order = %v, want %v", gotNames, want)
			}
		}
	}
}

func TestOrderedTransportCtorsWebSocketOnly(t *testing.T) {
	got := orderedTransportCtors(types.NewSet[TransportCtor](&WebSocketBuilder{}))
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	if got[0].Name() != transports.WEBSOCKET {
		t.Fatalf("transport = %s, want %s", got[0].Name(), transports.WEBSOCKET)
	}
}
