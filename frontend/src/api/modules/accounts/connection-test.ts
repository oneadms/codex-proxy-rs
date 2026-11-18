import type { AccountIdParam } from './types'
import { requestStream } from '../../request'

export async function* testAccountConnection(
  data: AccountIdParam & { modelId: string, prompt: string },
  signal: AbortSignal,
) {
  const response = await requestStream({
    url: '/api/admin/accounts/connection-test',
    method: 'POST',
    headers: { accept: 'text/event-stream' },
    data,
    signal,
    silent: true,
  }, {
    accept: meta => meta.status === 200 && meta.header('content-type')?.split(';', 1)[0] === 'text/event-stream',
    maximumBytes: 64 * 1024,
  })
  if (!response.body)
    return

  const reader = response.body.pipeThrough(new TextDecoderStream()).getReader()
  let pending = ''
  try {
    for (;;) {
      const { done, value } = await reader.read()
      if (done)
        return
      // 网关每帧只写一行 JSON，跳过保活注释
      const frames = (pending + value).split(/\r?\n\r?\n/)
      pending = frames.pop() ?? ''
      for (const frame of frames) {
        if (frame.startsWith('data:'))
          yield frame.slice('data:'.length)
      }
    }
  }
  finally {
    reader.releaseLock()
  }
}
