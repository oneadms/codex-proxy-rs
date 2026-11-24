-- 管理端开关只作用于保存后新建的 OpenAI 模型请求
alter table runtime_settings
    add column openai_telemetry_enabled boolean not null default true;
