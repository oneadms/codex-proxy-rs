import type { MihomoNode } from '@/api'
import type { RequestOptions } from '@/api/request'
import { computed, shallowRef, watch } from 'vue'
import { getMihomoNodes, testProxy } from '@/api'
import { ApiError } from '@/api/request'
import { useIdSet } from '@/composables/useIdSet'
import { useRequestState } from '@/composables/useRequestState'
import { t } from '@/i18n'
import { notification as ZNotification } from '@/utils/notification'
import { errorMessage } from '@/utils/operation'
import { useProxyBatchTest } from '@/views/proxies/composables/useProxyBatchTest'

export function useMihomoNodeTesting(options: {
  subscriptionId: () => string
  reload: (subscriptionId: string) => Promise<unknown>
}) {
  const testing = useIdSet<string>()
  const request = useRequestState()
  const batchSubscriptionId = shallowRef('')
  const detectingLocation = shallowRef(false)

  async function execute(nodes: MihomoNode[], requestOptions: RequestOptions, detectLocation: boolean) {
    for (const node of nodes)
      testing.add(node.id)
    try {
      return await testProxy(nodes.map(node => ({
        id: node.proxyId,
        revision: node.proxyRevision,
        detectLocation,
      })), requestOptions)
    }
    finally {
      for (const node of nodes)
        testing.remove(node.id)
    }
  }

  const tests = useProxyBatchTest<MihomoNode>(async (nodes, requestOptions, detectLocation) => {
    const results = await execute(nodes, requestOptions, detectLocation)
    await options.reload(batchSubscriptionId.value)
    return results
  })
  const busy = computed(() => request.loading.value || tests.running.value)
  const progress = computed(() => batchSubscriptionId.value === options.subscriptionId() && tests.items.value.length
    ? {
        total: tests.items.value.length,
        completed: tests.items.value.filter(item => !['pending', 'running'].includes(item.status)).length,
      }
    : null)

  async function testNode(node: MihomoNode, detectLocation: boolean) {
    if (busy.value || testing.has(node.id) || !node.available)
      return
    const subscriptionId = options.subscriptionId()
    try {
      const result = (await execute([node], {}, detectLocation))[0]
      if (result?.status !== 'succeeded') {
        ZNotification.warning({
          message: result?.status === 'failed'
            ? result.message
            : t('proxies.actions.testFailed'),
        })
        return
      }
      const test = result.data.lastTest
      if (!test?.success) {
        ZNotification.warning({ message: test?.message ?? t('proxies.actions.testFailed') })
      }
      else if (detectLocation && test.location.status !== 'detected') {
        ZNotification.warning({
          message: test.location.status === 'failed'
            ? test.location.message
            : t('proxies.region.regionalConflict'),
        })
      }
      else {
        ZNotification.success({
          message: t('mihomo.actions.nodeTested', { name: node.name, latencyMs: test.latencyMs }),
        })
      }
    }
    catch (error) {
      if (!(error instanceof ApiError)) {
        ZNotification.error({
          message: errorMessage(error, t('proxies.actions.testFailed')),
        })
      }
    }
    finally {
      await options.reload(subscriptionId)
    }
  }

  async function testAll(detectLocation: boolean) {
    const subscriptionId = options.subscriptionId()
    if (!subscriptionId || busy.value || testing.ids.value.size)
      return
    const requestId = request.start()
    batchSubscriptionId.value = subscriptionId
    detectingLocation.value = detectLocation
    tests.reset()
    try {
      const nodes: MihomoNode[] = []
      let totalPages = 1
      for (let page = 1; page <= totalPages; page += 1) {
        const result = await getMihomoNodes({ subscriptionId, page, pageSize: 200 }, { silent: true, signal: request.signal })
        if (!request.isCurrent(requestId))
          return
        if (page === 1)
          totalPages = result.page.totalPages
        nodes.push(...result.items.filter(node => node.available))
      }
      if (!nodes.length)
        return
      await tests.run(nodes, detectLocation)
      if (!request.isCurrent(requestId))
        return
      await options.reload(subscriptionId)
      const succeeded = tests.items.value.filter(item => item.status === 'succeeded').length
      const failed = tests.items.value.length - succeeded
      const locationsPending = detectLocation
        ? tests.items.value.filter(item => item.status === 'succeeded' && item.record?.lastTest?.location.status !== 'detected').length
        : 0
      const notify = failed || locationsPending
        ? ZNotification.warning
        : ZNotification.success
      notify({
        message: t('mihomo.actions.testsCompleted', { succeeded, failed, locationsPending }),
      })
    }
    catch (error) {
      request.fail(requestId, error)
      if (request.isCurrent(requestId) && request.error.value)
        ZNotification.error({ message: request.error.value })
    }
    finally {
      request.finish(requestId)
    }
  }

  watch(options.subscriptionId, () => {
    request.invalidate({ resetLoading: true })
    tests.stop()
  }, { flush: 'sync' })

  function stop() {
    tests.stop()
    if (!tests.running.value)
      request.invalidate({ resetLoading: true })
  }

  return {
    testingIds: testing.ids,
    busy,
    detectingLocation,
    progress,
    stopping: tests.stopping,
    stop,
    testNode,
    testAll,
  }
}
