-- 手动连接测试的默认提示词由全局运行设置统一保存
alter table runtime_settings
    add column account_connection_test_prompt text not null default 'Reply with exactly OK.',
    add constraint runtime_settings_connection_test_prompt_nonblank
        check (account_connection_test_prompt ~ '[^[:space:]]');
