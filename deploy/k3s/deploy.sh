#!/usr/bin/env bash
# 由专用 SSH 密钥的强制命令调用，只更新现有 production 应用
set -euo pipefail
umask 077
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

if [[ ${SSH_ORIGINAL_COMMAND:-} =~ ^deploy\ ([0-9a-f]{40})\ (sha256:[0-9a-f]{64})$ ]]; then
  revision=${BASH_REMATCH[1]}
  digest=${BASH_REMATCH[2]}
else
  printf '%s\n' 'Only a deployment revision and image digest are accepted' >&2
  exit 1
fi

IFS= read -r registry_user
IFS= read -r registry_token
[[ "$registry_user" =~ ^[A-Za-z0-9][A-Za-z0-9-]*$ && -n "$registry_token" ]]
image="ghcr.io/oneadms/codex-proxy-rs@$digest"

exec 9>/run/lock/cpr-ci-deploy.lock
flock -w 60 9

# 单节点预拉取到 CRI 所用命名空间，Pod 重建时无需保留短期令牌
[[ $(kubectl get nodes -o name | wc -l) -eq 1 ]]
k3s ctr --namespace k8s.io images pull --platform linux/amd64 \
  --user "$registry_user:$registry_token" "$image" >/dev/null
unset registry_token

backup_dir="/opt/codex-proxy-rs/.runtime/ci-backups/$(date -u +%Y%m%dT%H%M%SZ)-$revision"
install -d -m 0700 "$backup_dir"
kubectl -n production get deployment codex-proxy-rs -o json > "$backup_dir/deployment.json"
kubectl -n production exec statefulset/cpr-postgres -- sh -c \
  'export PGPASSWORD="$POSTGRES_PASSWORD"; exec pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" --format=custom' \
  > "$backup_dir/postgres.dump"
test -s "$backup_dir/postgres.dump"
printf 'Database backup: %s\n' "$backup_dir"

printf -v deployment_patch \
  '{"spec":{"template":{"metadata":{"annotations":{"cpr.custom/revision":"%s"}},"spec":{"containers":[{"name":"codex-proxy-rs","image":"%s","imagePullPolicy":"IfNotPresent","env":[{"name":"CPR_UPDATE_REPOSITORY","value":"oneadms/codex-proxy-rs"},{"name":"CPR_ENABLE_SELF_RESTART","value":"false"}]}]}}}}' \
  "$revision" "$image"
kubectl -n production patch deployment codex-proxy-rs --type=strategic --patch "$deployment_patch"

# 迁移可能已提交，失败时保留备份供人工恢复，不自动切回旧程序
if ! kubectl -n production rollout status deployment/codex-proxy-rs --timeout=600s; then
  kubectl -n production get pods -l app=codex-proxy-rs -o wide
  printf 'Deployment failed; inspect the application and backup at %s\n' "$backup_dir" >&2
  exit 1
fi

kubectl -n production exec deployment/codex-proxy-rs -- \
  curl --fail --silent --show-error --max-time 15 http://127.0.0.1:8080/readyz
kubectl -n production exec deployment/codex-proxy-rs -- /app/bin/mihomo -v
printf 'Deployed revision %s using %s\n' "$revision" "$image"
