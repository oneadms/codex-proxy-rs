// 验证 AnyTLS 活动流跨越远端空闲期限，以及关闭流后的心跳生命周期
package session

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/transport/anytls/padding"
)

func TestKeepAlivePreservesDelayedResponseAndStopsWhenIdle(t *testing.T) {
	clientConn, peerConn := net.Pipe()
	defer peerConn.Close()
	var factory atomic.Pointer[padding.PaddingFactory]
	factory.Store(padding.NewPaddingFactory([]byte("stop=0")))
	client := NewClientSession(clientConn, &factory, "test")
	defer client.Close()
	client.Run()

	peerDone := make(chan error, 1)
	go func() {
		var header rawHeader
		var sid uint32
		hearts := 0
		for {
			// 没有心跳时，在业务响应生成前触发远端空闲超时
			_ = peerConn.SetReadDeadline(time.Now().Add(3 * time.Second))
			if _, err := io.ReadFull(peerConn, header[:]); err != nil {
				peerDone <- err
				return
			}
			if _, err := io.CopyN(io.Discard, peerConn, int64(header.Length())); err != nil {
				peerDone <- err
				return
			}
			switch header.Cmd() {
			case cmdSYN:
				sid = header.StreamID()
			case cmdHeartRequest:
				hearts++
				if hearts == 2 {
					// 等过一次完整空闲期限后，再交付业务数据
					_, err := peerConn.Write([]byte{cmdPSH, byte(sid >> 24), byte(sid >> 16), byte(sid >> 8), byte(sid), 0, 2, 'o', 'k'})
					if err != nil {
						peerDone <- err
						return
					}
				}
			case cmdFIN:
				peerDone <- nil
				return
			}
		}
	}()

	stream, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte("request")); err != nil {
		t.Fatal(err)
	}
	_ = stream.SetReadDeadline(time.Now().Add(8 * time.Second))
	response := make([]byte, 2)
	if _, err := io.ReadFull(stream, response); err != nil {
		t.Fatal(err)
	}
	if string(response) != "ok" {
		t.Fatalf("unexpected response: %q", response)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-peerDone; err != nil {
		t.Fatal(err)
	}

	// 流结束后，保留在空闲池的会话不再发送周期心跳
	_ = peerConn.SetReadDeadline(time.Now().Add(2500 * time.Millisecond))
	var data [1]byte
	_, err = peerConn.Read(data[:])
	if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatalf("idle session sent data or closed unexpectedly: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	_ = peerConn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := peerConn.Read(data[:]); err != io.EOF {
		t.Fatalf("closed session still owns the connection: %v", err)
	}
}

type failedHeartbeatConn struct{ net.Conn }

func (failedHeartbeatConn) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestKeepAliveReleasesSessionOnWriteFailure(t *testing.T) {
	clientConn, peerConn := net.Pipe()
	defer peerConn.Close()
	var factory atomic.Pointer[padding.PaddingFactory]
	factory.Store(padding.NewPaddingFactory([]byte("stop=0")))
	client := NewClientSession(failedHeartbeatConn{clientConn}, &factory, "test")
	defer client.Close()
	stream := newStream(1, client)
	client.streams[1] = stream
	done := make(chan struct{})
	go func() {
		client.keepAlive()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("heartbeat worker retained a failed connection")
	}
	if !client.IsClosed() {
		t.Fatal("failed session remains reusable")
	}
	var data [1]byte
	if _, err := stream.Read(data[:]); err != net.ErrClosed {
		t.Fatalf("active stream did not observe connection failure: %v", err)
	}
}
