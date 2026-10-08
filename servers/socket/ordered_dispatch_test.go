package socket

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/zishang520/socket.io/v3/pkg/types"
)

// Exercise the wire decoder and listener delivery, not a handler-side lock.
func TestOrderedBinaryBurst(t *testing.T) {
	for _, transport := range []string{"polling", "websocket"} {
		t.Run(transport, func(t *testing.T) {
			server := NewServer(nil, nil)
			received := make(chan int, 200)
			middlewareEntered := make(chan struct{})
			release := make(chan struct{})
			defer close(release)
			_ = server.On("connection", func(a ...any) {
				s := a[0].(*Socket)
				s.Use(func(event []any, next func(error)) {
					if event[0] == "audio" && event[1].(types.BufferInterface).Bytes()[0] == 0 {
						close(middlewareEntered)
						go func() { <-release; next(nil) }()
					} else {
						next(nil)
					}
				})
				_ = s.On("audio", func(a ...any) { received <- int(a[0].(types.BufferInterface).Bytes()[0]) })
			})
			h := httptest.NewServer(server.ServeHandler(nil))
			defer h.Close()
			defer server.Close(nil)
			c := newOrderWire(t, h.URL, transport)
			c.sendText("40")
			if got := c.read(); !strings.HasPrefix(got, "40") {
				t.Fatalf("CONNECT: %s", got)
			}
			for i := range 100 {
				c.sendText(`451-["audio",{"_placeholder":true,"num":0}]`)
				c.sendBinary([]byte{byte(i)})
			}
			select {
			case <-middlewareEntered:
			case <-time.After(5 * time.Second):
				t.Fatal("middleware not entered")
			}
			// Use a separate signal: closing release is deferred so failure cannot leak a waiter.
			release <- struct{}{}
			for i := range 100 {
				select {
				case got := <-received:
					if got != i {
						t.Fatalf("packet %d arrived as %d", i, got)
					}
				case <-time.After(5 * time.Second):
					t.Fatalf("missing packet %d", i)
				}
			}
		})
	}
}

func TestLongOperationStopAndACK(t *testing.T) {
	for _, transport := range []string{"polling", "websocket"} {
		t.Run(transport, func(t *testing.T) {
			server := NewServer(nil, nil)
			_ = server.On("connection", func(a ...any) {
				s := a[0].(*Socket)
				stop := make(chan struct{})
				_ = s.On("start", func(a ...any) {
					ack := a[len(a)-1].(Ack)
					// Business adaptation owns asynchronous work, while setup stays ordered.
					go func() { <-stop; ack([]any{"stopped"}, nil) }()
				})
				_ = s.On("stop", func(a ...any) { close(stop); a[len(a)-1].(Ack)([]any{}, nil) })
				_ = s.On("callback", func(a ...any) {
					completed := make(chan struct{})
					s.Timeout(time.Second).Emit("question", Ack(func(v []any, err error) {
						if err != nil {
							t.Errorf("server callback: %v", err)
						}
						close(completed)
					}))
					// A server ACK must be processed even while this listener is waiting.
					<-completed
					a[len(a)-1].(Ack)([]any{"done"}, nil)
				})
			})
			h := httptest.NewServer(server.ServeHandler(nil))
			defer h.Close()
			defer server.Close(nil)
			c := newOrderWire(t, h.URL, transport)
			c.sendText("40")
			_ = c.read()
			c.sendText(`420["start"]`)
			c.sendText(`421["stop"]`)
			got := map[string]bool{c.read(): true, c.read(): true}
			if !got[`430["stopped"]`] || !got[`431[]`] {
				t.Fatalf("start/stop ACKs: %v", got)
			}
			c.sendText(`422["callback"]`)
			question := c.read()
			if !strings.HasSuffix(question, `["question"]`) {
				t.Fatalf("question: %s", question)
			}
			id := strings.TrimSuffix(strings.TrimPrefix(question, "42"), `["question"]`)
			c.sendText("43" + id + `["answer"]`)
			if got := c.read(); got != `432["done"]` {
				t.Fatalf("callback ACK: %s", got)
			}
		})
	}
}

type orderWire struct {
	t       *testing.T
	url     string
	conn    *websocket.Conn
	client  *http.Client
	pending []string
}

func newOrderWire(t *testing.T, url, transport string) *orderWire {
	t.Helper()
	c := &orderWire{t: t, url: url + "/socket.io/?EIO=4&transport=" + transport, client: &http.Client{Timeout: 5 * time.Second}}
	if transport == "websocket" {
		var err error
		c.conn, _, err = websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(c.url, "http"), nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.conn.Close() })
	}
	open := c.read()
	var handshake struct {
		SID string `json:"sid"`
	}
	if len(open) < 2 || open[0] != '0' {
		t.Fatalf("OPEN: %s", open)
	}
	if err := json.Unmarshal([]byte(open[1:]), &handshake); err != nil {
		t.Fatal(err)
	}
	c.url += "&sid=" + handshake.SID
	return c
}
func (c *orderWire) request(method, body string) string {
	c.t.Helper()
	req, err := http.NewRequest(method, c.url, strings.NewReader(body))
	if err != nil {
		c.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "text/plain;charset=UTF-8")
	resp, err := c.client.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		c.t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		c.t.Fatal(fmt.Sprintf("HTTP %d: %s", resp.StatusCode, b))
	}
	return string(b)
}
func (c *orderWire) sendText(s string) {
	c.t.Helper()
	if c.conn != nil {
		if err := c.conn.WriteMessage(websocket.TextMessage, []byte(s)); err != nil {
			c.t.Fatal(err)
		}
		return
	}
	c.request("POST", s)
}
func (c *orderWire) sendBinary(b []byte) {
	c.t.Helper()
	if c.conn != nil {
		if err := c.conn.WriteMessage(websocket.BinaryMessage, b); err != nil {
			c.t.Fatal(err)
		}
		return
	}
	c.request("POST", "b"+base64.StdEncoding.EncodeToString(b))
}
func (c *orderWire) read() string {
	c.t.Helper()
	if c.conn != nil {
		_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, b, err := c.conn.ReadMessage()
		if err != nil {
			c.t.Fatal(err)
		}
		return string(b)
	}
	if len(c.pending) == 0 {
		c.pending = strings.Split(c.request("GET", ""), "\x1e")
	}
	result := c.pending[0]
	c.pending = c.pending[1:]
	return result
}
