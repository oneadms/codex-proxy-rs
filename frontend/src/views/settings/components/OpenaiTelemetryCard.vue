<script setup lang="ts">
import { ZCard, ZSwitch } from '@codex-proxy/ui'
import { useI18n } from 'vue-i18n'

const { t } = useI18n({ useScope: 'global' })
const enabled = defineModel<boolean>('enabled', { required: true })
</script>

<template>
  <ZCard :title="t('settings.telemetry.title')" :description="t('settings.telemetry.description')">
    <ZSwitch v-model="enabled" :label="t('settings.telemetry.enabled')" />
    <p class="mt-3 text-z-sm text-z-text-secondary">
      {{ t('settings.telemetry.help') }}
    </p>
  </ZCard>
</template>
