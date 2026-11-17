import type { RequestOptions } from '../request'
import type { OutboundProxyRecord } from './proxies'
import request from '../request'

export interface MihomoSubscription {
  id: string
  name: string
  hasUrl: boolean
  nodeCount: number
  revision: number
  updatedAt: string
  updatedAtDisplay: string
}

export type MihomoNodeSort = 'name' | 'latencyAsc' | 'latencyDesc'

export interface MihomoNode extends Pick<OutboundProxyRecord, 'autoLocation' | 'location' | 'detectedLocation' | 'lastTest' | 'lastTestAt' | 'lastTestAtDisplay'> {
  id: string
  proxyId: string
  name: string
  type: string
  available: boolean
  accountCount: number
  endpoint: string
  proxyRevision: number
}

interface MihomoSubscriptionPage {
  items: MihomoSubscription[]
}

interface MihomoNodePage {
  items: MihomoNode[]
  page: {
    page: number
    pageSize: number
    total: number
    totalPages: number
  }
}

interface MihomoSubscriptionMutation {
  subscription: MihomoSubscription
  nodeCount: number
  configRevision: number
}

interface MihomoSubscriptionRevision {
  id: string
  revision: number
}

export function getMihomoSubscriptions(options: RequestOptions = {}) {
  return request<MihomoSubscriptionPage>({
    url: '/api/admin/mihomo/subscriptions',
    method: 'GET',
    ...options,
  })
}

export function createMihomoSubscription(data: { name: string, url: string }) {
  return request<MihomoSubscription>({
    url: '/api/admin/mihomo/subscriptions/create',
    method: 'POST',
    data,
  })
}

export function updateMihomoSubscription(data: MihomoSubscriptionRevision & { name?: string, url?: string }) {
  return request<MihomoSubscription>({
    url: '/api/admin/mihomo/subscriptions/update',
    method: 'POST',
    data,
  })
}

export function refreshMihomoSubscription(data: MihomoSubscriptionRevision) {
  return request<MihomoSubscriptionMutation>({
    url: '/api/admin/mihomo/subscriptions/refresh',
    method: 'POST',
    data,
    timeout: 120000,
  })
}

export function deleteMihomoSubscription(data: MihomoSubscriptionRevision) {
  return request<null>({
    url: '/api/admin/mihomo/subscriptions/delete',
    method: 'POST',
    data,
  })
}

export function getMihomoNodes(data: { subscriptionId: string, page: number, pageSize: number, search?: string, sort?: MihomoNodeSort }, options: RequestOptions = {}) {
  return request<MihomoNodePage>({
    url: '/api/admin/mihomo/nodes',
    method: 'GET',
    params: data,
    ...options,
  })
}
