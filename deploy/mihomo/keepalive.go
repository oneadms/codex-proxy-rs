// 活动 AnyTLS 会话的协议心跳，避免代理链路在模型等待期间按空闲连接关闭
package session

import "time"

func (s *Session) keepAlive() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.die:
			return
		case <-ticker.C:
			s.streamLock.RLock()
			active := len(s.streams) > 0
			s.streamLock.RUnlock()
			// 空闲池继续遵守原来的回收策略，心跳仅维持尚未结束的业务流
			if active {
				if _, err := s.writeControlFrame(newFrame(cmdHeartRequest, 0)); err != nil {
					return
				}
			}
		}
	}
}
