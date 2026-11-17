<script setup lang="ts">
import type { MihomoSubscription } from '@/api/modules/mihomo'
import { ZFormItem, ZSelect, ZTextarea } from '@codex-proxy/ui'
import { computed, onMounted, shallowRef } from 'vue'
import { useI18n } from 'vue-i18n'
import { getMihomoSubscriptions } from '@/api/modules/mihomo'
import { useRequestState } from '@/composables/useRequestState'

defineProps<{ disabled: boolean }>()
const text = defineModel<string>({ required: true })
const nodePoolId = defineModel<string>('nodePoolId', { required: true })
const { t } = useI18n({ useScope: 'global' })
const subscriptions = shallowRef<MihomoSubscription[]>([])
const request = useRequestState()
const { loading } = request
const options = computed(() => subscriptions.value.map(subscription => ({
  value: subscription.id,
  label: subscription.name,
  disabled: subscription.nodeCount === 0,
})))

onMounted(async () => {
  const version = request.start()
  try {
    const result = await getMihomoSubscriptions({ signal: request.signal })
    if (request.isCurrent(version))
      subscriptions.value = result.items
  }
  catch {}
  finally {
    request.finish(version)
  }
})
</script>

<template>
  <div class="grid gap-4">
    <ZFormItem :label="t('accounts.protocolLogin.pool')" required>
      <ZSelect
        v-model="nodePoolId"
        class="w-full"
        :options="options"
        :placeholder="t('accounts.protocolLogin.poolRequired')"
        :disabled="disabled || loading"
        :aria-label="t('accounts.protocolLogin.pool')"
      />
      <p class="mt-2 text-z-xs text-z-text-secondary">
        {{ t('accounts.protocolLogin.allocation') }}
      </p>
    </ZFormItem>
    <ZFormItem :label="t('accounts.protocolLogin.accounts')" required>
      <ZTextarea
        v-model="text"
        :rows="9"
        :disabled="disabled"
        :aria-label="t('accounts.protocolLogin.accounts')"
        placeholder="username----password----2fa"
        autocomplete="off"
        spellcheck="false"
      />
    </ZFormItem>
    <p class="m-0 text-z-xs text-z-warning-text" role="note">
      {{ t('accounts.protocolLogin.revokeNotice') }}
    </p>
  </div>
</template>
