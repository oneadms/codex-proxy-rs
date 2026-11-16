create table mihomo_subscriptions (
    id text primary key,
    name text not null check (char_length(name) between 1 and 100),
    url text not null,
    revision bigint not null default 1 check (revision > 0),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table mihomo_nodes (
    id text primary key,
    subscription_id text not null references mihomo_subscriptions(id) on delete cascade,
    source_name text not null check (char_length(source_name) between 1 and 100),
    kind text not null,
    proxy_id text not null unique references outbound_proxies(id) on delete restrict,
    listener_port integer not null unique check (listener_port between 20000 and 39999),
    config_json jsonb not null,
    available boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (subscription_id, source_name)
);
