<script setup lang="ts">
import type { FormRules } from '@codex-proxy/ui'
import type { MihomoNode, MihomoSubscription } from '@/api'

import { defineTableColumns, ZButton, ZCard, ZConfirmDialog, ZDialog, ZForm, ZFormItem, ZIconButton, ZInput, ZPageHeader, ZPagination, ZTable, ZTag } from '@codex-proxy/ui'
import { Eye, Pencil, Plus, RefreshCw, Search, Trash2, Users } from '@lucide/vue'
import { computed, onMounted, reactive, shallowRef, useTemplateRef, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { createMihomoSubscription, deleteMihomoSubscription, getMihomoNodes, getMihomoSubscriptions, refreshMihomoSubscription, updateMihomoSubscription } from '@/api'
import { useAsyncAction } from '@/composables/useAsyncAction'
import { usePagedQuery } from '@/composables/usePagedQuery'
import { useRequestState } from '@/composables/useRequestState'
import { notification as ZNotification } from '@/utils/notification'
import ProxyAccountsModal from '@/views/proxies/components/ProxyAccountsModal.vue'

const { t } = useI18n({ useScope: 'global' })

const subscriptions = shallowRef<MihomoSubscription[]>([])
const selectedSubscriptionId = shallowRef('')
const listRequest = useRequestState()
const operation = useAsyncAction()
const refreshingId = shallowRef('')
const nodeSearch = shallowRef('')
const showEditor = shallowRef(false)
const editingSubscription = shallowRef<MihomoSubscription | null>(null)
const pendingDelete = shallowRef<MihomoSubscription | null>(null)
const showDelete = shallowRef(false)
const showAccounts = shallowRef(false)
const inspectedNode = shallowRef<MihomoNode | null>(null)
const form = reactive({ name: '', url: '' })
const editorForm = useTemplateRef<InstanceType<typeof ZForm>>('editorForm')

const selectedSubscription = computed(() => subscriptions.value.find(subscription => subscription.id === selectedSubscriptionId.value) ?? null)
const accountProxy = computed(() => {
  const node = inspectedNode.value
  return node
    ? {
        id: node.proxyId,
        name: node.name,
        enabled: true,
        available: node.available,
        accountCount: node.accountCount,
      }
    : null
})
const editorTitle = computed(() => editingSubscription.value
  ? t('mihomo.editor.editTitle')
  : t('mihomo.editor.createTitle'))
const editorRules = computed<FormRules>(() => ({
  name: {
    required: true,
    whitespace: true,
    trigger: ['blur', 'change'],
    message: t('mihomo.editor.nameRequired'),
  },
  url: {
    required: !editingSubscription.value,
    whitespace: true,
    transform: value => value.trim(),
    trigger: ['blur', 'change'],
    message: t('mihomo.editor.urlRequired'),
  },
}))

const nodeQuery = usePagedQuery({
  initialPageSize: 20,
  load: (pagination, options) => getMihomoNodes({
    ...pagination,
    subscriptionId: selectedSubscriptionId.value,
    search: nodeSearch.value.trim() || undefined,
  }, options),
})
const nodePagination = computed(() => ({
  currentPage: nodeQuery.page.value,
  pageSize: nodeQuery.pageSize.value,
  total: nodeQuery.total.value,
}))
const nodeEmptyText = computed(() => nodeQuery.error.value
  || (nodeSearch.value.trim()
    ? t('mihomo.page.noNodeMatches')
    : selectedSubscriptionId.value
      ? t('mihomo.page.emptyNodes')
      : t('mihomo.page.notSelected')))

const subscriptionColumns = computed(() => defineTableColumns<MihomoSubscription>([
  {
    key: 'name',
    label: t('mihomo.columns.subscriptionName'),
    kind: 'identity',
    size: 'xl',
  },
  {
    key: 'nodeCount',
    label: t('mihomo.columns.nodeCount'),
    kind: 'custom',
    size: 'sm',
    align: 'center',
  },
  {
    key: 'urlState',
    label: t('mihomo.columns.subscriptionUrl'),
    kind: 'custom',
    size: 'md',
    align: 'center',
  },
  {
    key: 'updatedAt',
    label: t('mihomo.columns.updatedAt'),
    kind: 'custom',
    fixedWidth: true,
  },
  {
    key: 'actions',
    label: t('common.fields.actions'),
    kind: 'actions',
    size: 'xl',
    fixedWidth: true,
  },
]))
const nodeColumns = computed(() => defineTableColumns<MihomoNode>([
  {
    key: 'name',
    label: t('mihomo.columns.nodeName'),
    kind: 'identity',
    size: 'lg',
  },
  {
    key: 'type',
    label: t('mihomo.columns.type'),
    kind: 'custom',
    size: 'sm',
  },
  {
    key: 'endpoint',
    label: t('mihomo.columns.endpoint'),
    kind: 'custom',
    size: 'xl',
  },
  {
    key: 'available',
    label: t('mihomo.columns.availability'),
    kind: 'status',
    size: 'sm',
  },
  {
    key: 'accountCount',
    label: t('mihomo.columns.accounts'),
    kind: 'custom',
    size: 'sm',
    align: 'center',
  },
]))

async function loadSubscriptions(silent = false) {
  const requestId = listRequest.start(silent)
  const options = { silent, signal: listRequest.signal }
  try {
    const result = await getMihomoSubscriptions(options)
    if (!listRequest.isCurrent(requestId))
      return
    subscriptions.value = result.items
    if (!result.items.some(subscription => subscription.id === selectedSubscriptionId.value))
      selectedSubscriptionId.value = result.items[0]?.id ?? ''
  }
  catch (cause) {
    listRequest.fail(requestId, cause, silent)
  }
  finally {
    listRequest.finish(requestId)
  }
}

function selectSubscription(subscriptionId: string) {
  if (selectedSubscriptionId.value === subscriptionId) {
    void nodeQuery.reloadFromStart()
    return
  }
  selectedSubscriptionId.value = subscriptionId
}

function openCreate() {
  editingSubscription.value = null
  form.name = ''
  form.url = ''
  showEditor.value = true
}

function openEdit(subscription: MihomoSubscription) {
  editingSubscription.value = subscription
  form.name = subscription.name
  form.url = ''
  showEditor.value = true
}

async function saveSubscription() {
  if (operation.loading.value || !await editorForm.value?.validate().catch(() => false))
    return
  const editing = editingSubscription.value
  const name = form.name.trim()
  const url = form.url.trim()
  const saved = await operation.run(async () => {
    const result = editing
      ? await updateMihomoSubscription({
          id: editing.id,
          revision: editing.revision,
          name,
          ...(url
            ? { url }
            : {}),
        })
      : await createMihomoSubscription({ name, url })
    await loadSubscriptions(true)
    if (editing && url && selectedSubscriptionId.value === result.id)
      await nodeQuery.reloadFromStart()
    else if (selectedSubscriptionId.value !== result.id)
      selectedSubscriptionId.value = result.id
    return result
  })
  if (!saved)
    return
  showEditor.value = false
  ZNotification.success({
    message: t('mihomo.actions.saved'),
  })
}

async function refreshSubscription(subscription: MihomoSubscription) {
  if (operation.loading.value || !subscription.hasUrl)
    return
  refreshingId.value = subscription.id
  const result = await operation.run(() => refreshMihomoSubscription({
    id: subscription.id,
    revision: subscription.revision,
  }))
  refreshingId.value = ''
  if (!result)
    return
  subscriptions.value = subscriptions.value.map(item => item.id === subscription.id
    ? { ...result.subscription, nodeCount: result.nodeCount }
    : item)
  await loadSubscriptions(true)
  if (selectedSubscriptionId.value === subscription.id)
    await nodeQuery.reloadFromStart()
  ZNotification.success({
    message: t('mihomo.actions.refreshed'),
  })
}

function requestDelete(subscription: MihomoSubscription) {
  pendingDelete.value = subscription
  showDelete.value = true
}

async function confirmDelete() {
  const subscription = pendingDelete.value
  if (!showDelete.value || !subscription || operation.loading.value)
    return
  const deleted = await operation.run(async () => {
    await deleteMihomoSubscription({
      id: subscription.id,
      revision: subscription.revision,
    })
    return true
  })
  if (!deleted)
    return
  showDelete.value = false
  await loadSubscriptions(true)
  ZNotification.success({
    message: t('mihomo.actions.deleted'),
  })
}

function viewNodeAccounts(node: MihomoNode) {
  inspectedNode.value = node
  showAccounts.value = true
}

async function reloadAfterAccountChange() {
  await Promise.all([
    loadSubscriptions(true),
    nodeQuery.execute(),
  ])
}

watch(selectedSubscriptionId, (subscriptionId) => {
  nodeQuery.invalidate({ resetLoading: true })
  if (subscriptionId) {
    void nodeQuery.reloadFromStart()
  }
  else {
    nodeQuery.items.value = []
    nodeQuery.total.value = 0
  }
}, { flush: 'sync' })

watch(nodeSearch, (_value, _previous, onCleanup) => {
  if (!selectedSubscriptionId.value)
    return
  const timer = setTimeout(() => {
    void nodeQuery.reloadFromStart()
  }, 300)
  onCleanup(() => clearTimeout(timer))
})

watch(showEditor, (open) => {
  editorForm.value?.clearValidate()
  if (!open)
    form.url = ''
})

onMounted(() => void loadSubscriptions())
</script>

<template>
  <div class="flex h-full min-h-0 w-full flex-col overflow-hidden">
    <ZPageHeader
      :title="t('navigation.mihomo')"
      :description="t('mihomo.page.description')"
    />

    <div class="mt-5 grid min-h-0 flex-1 gap-4 lg:grid-rows-[minmax(13rem,0.85fr)_minmax(16rem,1.15fr)]">
      <ZCard class="flex min-h-52 flex-col lg:min-h-0">
        <template #header>
          <div class="flex w-full flex-wrap items-center gap-3">
            <h2 class="m-0 text-z font-heavy text-z-text">
              {{ t('mihomo.page.subscriptions') }}
            </h2>
            <ZButton type="primary" class="ml-auto" :disabled="operation.loading.value" @click="openCreate">
              <template #icon>
                <Plus class="size-4" />
              </template>
              {{ t('mihomo.actions.create') }}
            </ZButton>
          </div>
        </template>
        <template #body>
          <ZTable
            class="min-h-0 flex-1"
            :columns="subscriptionColumns"
            :data="subscriptions"
            :loading="listRequest.loading.value"
            :empty-text="listRequest.error.value || t('mihomo.page.emptySubscriptions')"
          >
            <template #name="{ row }">
              <button
                type="button"
                class="max-w-full cursor-pointer truncate border-0 bg-transparent p-0 text-left font-heavy text-z-text outline-none hover:text-z-primary-text focus-visible:ring-2 focus-visible:ring-z-control-outline"
                :aria-current="row.id === selectedSubscriptionId ? 'true' : undefined"
                @click="selectSubscription(row.id)"
              >
                {{ row.name }}
              </button>
            </template>
            <template #nodeCount="{ row }">
              <span class="font-mono tabular-nums">{{ row.nodeCount }}</span>
            </template>
            <template #urlState="{ row }">
              <ZTag :type="row.hasUrl ? 'success' : 'default'">
                {{ row.hasUrl ? t('common.status.available') : t('common.values.unspecified') }}
              </ZTag>
            </template>
            <template #updatedAt="{ row }">
              {{ row.updatedAtDisplay }}
            </template>
            <template #actions="{ row }">
              <div class="flex items-center justify-end gap-1">
                <ZIconButton
                  size="small"
                  :title="t('mihomo.actions.viewNodes')"
                  :aria-label="t('mihomo.actions.viewNodes')"
                  :disabled="operation.loading.value"
                  @click="selectSubscription(row.id)"
                >
                  <Eye class="size-3.5" />
                </ZIconButton>
                <ZIconButton
                  size="small"
                  :title="t('mihomo.actions.refresh')"
                  :aria-label="t('mihomo.actions.refresh')"
                  :loading="refreshingId === row.id"
                  :disabled="operation.loading.value || !row.hasUrl"
                  @click="refreshSubscription(row)"
                >
                  <RefreshCw class="size-3.5" />
                </ZIconButton>
                <ZIconButton
                  size="small"
                  :title="t('mihomo.actions.edit')"
                  :aria-label="t('mihomo.actions.edit')"
                  :disabled="operation.loading.value"
                  @click="openEdit(row)"
                >
                  <Pencil class="size-3.5" />
                </ZIconButton>
                <ZIconButton
                  size="small"
                  type="danger"
                  :title="t('mihomo.actions.delete')"
                  :aria-label="t('mihomo.actions.delete')"
                  :disabled="operation.loading.value"
                  @click="requestDelete(row)"
                >
                  <Trash2 class="size-3.5" />
                </ZIconButton>
              </div>
            </template>
          </ZTable>
        </template>
      </ZCard>

      <ZCard class="flex min-h-0 flex-col">
        <template #header>
          <div class="flex w-full flex-wrap items-center gap-3">
            <div class="min-w-0 flex-1">
              <h2 class="m-0 text-z font-heavy text-z-text">
                {{ t('mihomo.page.nodes') }}
              </h2>
              <p class="m-0 mt-1 truncate text-z-xs text-z-text-secondary" :title="selectedSubscription?.name">
                {{ selectedSubscription?.name ?? t('mihomo.page.notSelected') }}
              </p>
            </div>
            <ZInput
              v-model="nodeSearch"
              class="min-w-0 sm:w-80"
              :disabled="!selectedSubscriptionId"
              :aria-label="t('mihomo.page.searchNodes')"
              :placeholder="t('mihomo.page.searchPlaceholder')"
            >
              <template #prefix>
                <Search class="size-4.5 text-z-text-tertiary" />
              </template>
            </ZInput>
          </div>
        </template>
        <template #body>
          <div class="flex h-full min-h-0 flex-col">
            <ZTable
              :key="selectedSubscriptionId"
              class="min-h-0 flex-1"
              :columns="nodeColumns"
              :data="nodeQuery.items.value"
              :loading="nodeQuery.loading.value"
              :empty-text="nodeEmptyText"
            >
              <template #name="{ row }">
                <span class="block truncate text-z font-heavy text-z-text" :title="row.name">{{ row.name }}</span>
              </template>
              <template #type="{ row }">
                <span class="text-z-sm text-z-text-secondary">{{ row.type }}</span>
              </template>
              <template #endpoint="{ row }">
                <span class="block truncate font-mono text-z-xs text-z-text-secondary" :title="row.endpoint">{{ row.endpoint }}</span>
              </template>
              <template #available="{ row }">
                <ZTag :type="row.available ? 'success' : 'default'">
                  {{ row.available ? t('mihomo.page.available') : t('mihomo.page.unavailable') }}
                </ZTag>
              </template>
              <template #accountCount="{ row }">
                <ZButton
                  variant="link"
                  size="small"
                  class="h-auto! w-fit rounded-sm! p-0! text-z-sm! leading-normal! font-normal! no-underline! hover:text-z-primary-text! [&>span]:gap-1.5"
                  :aria-label="t('mihomo.page.viewAccounts', { name: row.name, count: row.accountCount })"
                  @click="viewNodeAccounts(row)"
                >
                  <Users class="size-3.5" aria-hidden="true" />
                  <span class="font-mono tabular-nums">{{ row.accountCount }}</span>
                </ZButton>
              </template>
            </ZTable>
            <ZPagination
              v-bind="nodePagination"
              :disabled="nodeQuery.loading.value || !selectedSubscriptionId"
              @current-change="page => nodeQuery.execute({ page })"
              @size-change="pageSize => nodeQuery.execute({ page: 1, pageSize })"
            />
          </div>
        </template>
      </ZCard>
    </div>

    <ZDialog
      v-model="showEditor"
      :title="editorTitle"
      width="36rem"
      :show-close="!operation.loading.value"
      :close-on-click-modal="!operation.loading.value"
      :close-on-press-escape="!operation.loading.value"
    >
      <ZForm ref="editorForm" :model="form" :rules="editorRules" scroll-to-error focus-on-error novalidate @submit="saveSubscription">
        <ZFormItem prop="name" :show-message="false" :label="t('mihomo.editor.name')" required>
          <ZInput v-model="form.name" maxlength="100" :disabled="operation.loading.value" :aria-label="t('mihomo.editor.name')" :placeholder="t('mihomo.editor.namePlaceholder')" />
        </ZFormItem>
        <ZFormItem prop="url" :show-message="false" :label="t('mihomo.editor.url')" :required="!editingSubscription" :description="t('mihomo.editor.urlHelp')">
          <ZInput
            v-model="form.url"
            show-password
            autocomplete="new-password"
            :disabled="operation.loading.value"
            :aria-label="t('mihomo.editor.url')"
            :placeholder="editingSubscription ? t('mihomo.editor.urlOptionalPlaceholder') : t('mihomo.editor.urlPlaceholder')"
          />
        </ZFormItem>
      </ZForm>
      <template #footer>
        <ZButton :disabled="operation.loading.value" @click="showEditor = false">
          {{ t('common.actions.cancel') }}
        </ZButton>
        <ZButton type="primary" :loading="operation.loading.value" @click="saveSubscription">
          {{ t('mihomo.editor.save') }}
        </ZButton>
      </template>
    </ZDialog>

    <ZConfirmDialog
      v-model="showDelete"
      :title="t('mihomo.actions.delete')"
      :confirm-text="t('mihomo.actions.confirmDelete')"
      destructive
      :loading="operation.loading.value"
      @confirm="confirmDelete"
    >
      <p class="m-0 wrap-break-word">
        {{ t('mihomo.actions.deleteDescription', { name: pendingDelete?.name }) }}
      </p>
    </ZConfirmDialog>

    <ProxyAccountsModal v-model="showAccounts" :proxy="accountProxy" @changed="reloadAfterAccountChange" />
  </div>
</template>
