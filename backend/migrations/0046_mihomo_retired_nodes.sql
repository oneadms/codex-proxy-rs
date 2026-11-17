alter table mihomo_nodes add column retired boolean not null default false;
alter table mihomo_nodes add constraint mihomo_nodes_retired_unavailable check (not retired or not available);
alter table mihomo_nodes drop constraint mihomo_nodes_subscription_id_source_name_key;
create unique index mihomo_nodes_current_source_name_idx
    on mihomo_nodes (subscription_id, source_name) where not retired;
