package display

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing/common/buf"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/swipt-automation/swipt/internal/coredevice/tunnel"
	"github.com/swipt-automation/swipt/internal/coredevice/xpc"
)

const keyFrameInterval = time.Second

type Stream struct {
	client   *Client
	info     StreamInfo
	conn     *tunnel.UDPConn
	control  *xpc.Conn
	deviceIP netip.Addr

	datagrams []tunnel.Datagram

	keyFrameAccess sync.Mutex
	lastKeyFrame   time.Time
	keyFrameTimer  *time.Timer
	firSequence    uint8

	lastRTCP           atomic.Int64
	diagnosticReceived atomic.Uint64
	diagnosticFiltered atomic.Uint64
	diagnosticRTP      atomic.Uint64
	diagnosticRTCP     atomic.Uint64
	diagnosticBytes    atomic.Uint64

	done      chan struct{}
	endOnce   sync.Once
	endErr    error
	closeOnce sync.Once
	sendErr   error
	closeDone chan struct{}
	closeErr  error
	localOnce sync.Once
}

func newStream(client *Client, info StreamInfo, conn *tunnel.UDPConn, control *xpc.Conn) *Stream {
	stream := &Stream{
		client:    client,
		info:      info,
		conn:      conn,
		control:   control,
		deviceIP:  info.DeviceAddress.Addr().Unmap(),
		done:      make(chan struct{}),
		closeDone: make(chan struct{}),
	}
	stream.lastRTCP.Store(time.Now().UnixNano())
	go stream.watchControl()
	go stream.logDiagnosticCounters()
	go func() {
		var err error
		select {
		case <-client.device.Done():
			err = client.device.Err()
		case <-client.owner.Done():
			err = context.Cause(client.owner)
		case <-stream.done:
			return
		}
		stream.end(err)
		stream.closeLocal()
	}()
	return stream
}

func (s *Stream) Info() StreamInfo {
	return s.info
}

// Aggregate metadata only; never log packet payloads or peer addresses.
func (s *Stream) logDiagnosticCounters() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for report := 0; report < 5; report++ {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.client.logger.Debug("video-diag SSRC ", s.info.SSRC,
				" received=", s.diagnosticReceived.Load(),
				" source_filtered=", s.diagnosticFiltered.Load(),
				" rtp=", s.diagnosticRTP.Load(), " rtcp=", s.diagnosticRTCP.Load(),
				" bytes=", s.diagnosticBytes.Load())
		}
	}
}

func (s *Stream) watchControl() {
	for {
		message, err := s.control.Receive()
		if err != nil {
			select {
			case <-s.done:
			default:
				s.client.logger.Debug("display stream ", s.info.SSRC, " control connection ended: ", err)
			}
			s.end(E.Cause(err, "receive stream control"))
			s.closeLocal()
			return
		}
		s.client.logger.Debug("display stream ", s.info.SSRC, " control message: ", fmt.Sprint(message))
	}
}

func (s *Stream) closeLocal() {
	s.localOnce.Do(func() {
		_ = s.control.Close()
		_ = s.conn.Close()
	})
}

func (s *Stream) end(err error) {
	s.endOnce.Do(func() {
		s.endErr = err
		close(s.done)
		s.conn.CloseRead()
	})
}

type Packet struct {
	Buffer  *buf.Buffer
	RTCP    bool
	Arrival time.Time
}

func (s *Stream) ReadPackets(ctx context.Context, packets []Packet) (int, error) {
	if len(s.datagrams) < len(packets) {
		s.datagrams = make([]tunnel.Datagram, len(packets))
	}
	for {
		if err := context.Cause(ctx); err != nil {
			return 0, err
		}
		select {
		case <-s.done:
			return 0, s.endErr
		default:
		}
		n, err := s.conn.ReadBatch(ctx, s.datagrams[:len(packets)])
		if err != nil {
			if cause := context.Cause(ctx); cause != nil {
				return 0, cause
			}
			select {
			case <-s.done:
				return 0, s.endErr
			default:
			}
			return 0, err
		}
		count := 0
		for _, datagram := range s.datagrams[:n] {
			s.diagnosticReceived.Add(1)
			s.diagnosticBytes.Add(uint64(datagram.Buffer.Len()))
			if datagram.Source.Addr() != s.deviceIP {
				s.diagnosticFiltered.Add(1)
				datagram.Buffer.Release()
				continue
			}
			packets[count] = Packet{Buffer: datagram.Buffer, RTCP: isRTCP(datagram.Buffer.Bytes()), Arrival: datagram.Arrived}
			if packets[count].RTCP {
				s.diagnosticRTCP.Add(1)
			} else {
				s.diagnosticRTP.Add(1)
			}
			count++
		}
		if count > 0 {
			return count, nil
		}
	}
}

func (s *Stream) WriteRTCP(packet []byte) error {
	select {
	case <-s.done:
		return s.endErr
	default:
	}
	_, err := s.conn.WriteTo(packet, s.info.DeviceAddress)
	if err != nil {
		return E.Cause(err, "write rtcp")
	}
	if len(packet) >= 2 && (packet[1] == 200 || packet[1] == 201 || packet[1] == 204) {
		s.lastRTCP.Store(time.Now().UnixNano())
	}
	return nil
}

func (s *Stream) RequestKeyFrame() error {
	s.keyFrameAccess.Lock()
	defer s.keyFrameAccess.Unlock()
	select {
	case <-s.done:
		return net.ErrClosed
	default:
	}
	if s.keyFrameTimer != nil {
		return nil
	}
	if wait := keyFrameInterval - time.Since(s.lastKeyFrame); wait > 0 {
		s.keyFrameTimer = time.AfterFunc(wait, s.sendDelayedKeyFrame)
		return nil
	}
	return s.sendKeyFrameLocked()
}

func (s *Stream) sendDelayedKeyFrame() {
	s.keyFrameAccess.Lock()
	defer s.keyFrameAccess.Unlock()
	s.keyFrameTimer = nil
	select {
	case <-s.done:
		return
	default:
	}
	err := s.sendKeyFrameLocked()
	if err != nil {
		s.client.logger.Debug(E.Cause(err, "request a keyframe for display stream ", s.info.SSRC))
	}
}

func (s *Stream) sendKeyFrameLocked() error {
	s.lastKeyFrame = time.Now()
	s.firSequence++
	return s.WriteRTCP(keyFrameRequest(s.info.SSRC, s.info.DeviceSSRC, s.firSequence))
}

// pymobiledevice3 uses this empty-CNAME SDES in _build_rtcp_sdes
// (pymobiledevice3 1146717,
// pymobiledevice3/remote/core_device/screen_stream.py:1305-1322).
func keepaliveReport(ssrc uint32) []byte {
	report := binary.BigEndian.AppendUint32([]byte{0x80, 201, 0, 1}, ssrc)
	report = append(report, 0x81, 202, 0, 2)
	report = binary.BigEndian.AppendUint32(report, ssrc)
	return append(report, 1, 0, 0, 0)
}

func (s *Stream) startKeepalive() error {
	timeout := rtcpTimeout
	if s.info.RTCPTimeout > 0 {
		timeout = min(timeout, s.info.RTCPTimeout)
	}
	interval := timeout / 6
	if interval <= 0 {
		return E.New("display: invalid RTCP report interval")
	}
	report := keepaliveReport(s.info.SSRC)
	if err := s.WriteRTCP(report); err != nil {
		return err
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.done:
				return
			case now := <-ticker.C:
				if now.Sub(time.Unix(0, s.lastRTCP.Load())) < interval {
					continue
				}
				err := s.WriteRTCP(report)
				if err != nil {
					s.client.logger.Debug(E.Cause(err, "keepalive display stream ", s.info.SSRC))
				}
			}
		}
	}()
	return nil
}

func (s *Stream) Close() error {
	s.closeOnce.Do(func() {
		s.end(net.ErrClosed)
		s.keyFrameAccess.Lock()
		if s.keyFrameTimer != nil {
			s.keyFrameTimer.Stop()
			s.keyFrameTimer = nil
		}
		s.keyFrameAccess.Unlock()
		reply, err := s.client.stop(s.info.SSRC)
		if err != nil {
			s.sendErr = E.Cause(err, "stop ", string(s.info.Type), " stream")
			s.closeErr = s.sendErr
			s.closeLocal()
			close(s.closeDone)
			return
		}
		go func() {
			defer close(s.closeDone)
			defer s.closeLocal()
			stopped, err := reply()
			if err != nil {
				s.closeErr = E.Cause(err, "stop ", string(s.info.Type), " stream")
				return
			}
			s.client.logger.Debug("display stream ", s.info.SSRC, " stopped: ", fmt.Sprint(stopped))
		}()
	})
	return s.sendErr
}

func (s *Stream) WaitStopped() error {
	<-s.closeDone
	return s.closeErr
}
