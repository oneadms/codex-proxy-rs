<script setup lang="ts">
import { ZCard, ZFormItem, ZTextarea } from '@codex-proxy/ui'
import { useI18n } from 'vue-i18n'

const { t } = useI18n({ useScope: 'global' })
const prompt = defineModel<string>({ required: true })
</script>

<template>
  <ZCard :title="t('settings.connectionTest.title')">
    <ZFormItem
      prop="accountConnectionTestPrompt"
      :label="t('settings.connectionTest.defaultPrompt')"
      :description="t('settings.connectionTest.promptHelp')"
      required
      class="max-w-6xl"
    >
      <ZTextarea
        v-model="prompt"
        :aria-label="t('settings.connectionTest.defaultPrompt')"
        :rows="3"
      />
    </ZFormItem>
  </ZCard>
</template>
