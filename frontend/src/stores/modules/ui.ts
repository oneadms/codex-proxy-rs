import { defineStore } from 'pinia'
import { shallowRef } from 'vue'

export const useUiStore = defineStore(
  'ui',
  () => {
    const sidebarCollapsed = shallowRef(false)
    const accountConnectionTestPrompt = shallowRef('Reply with exactly OK.')

    function toggleSidebar(): void {
      sidebarCollapsed.value = !sidebarCollapsed.value
    }

    return {
      sidebarCollapsed,
      accountConnectionTestPrompt,
      toggleSidebar,
    }
  },
  {
    persist: {
      key: 'codex-proxy-rs-ui',
      pick: ['sidebarCollapsed', 'accountConnectionTestPrompt'],
    },
  },
)
