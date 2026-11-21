#!/usr/bin/env bash
# S0: register a lab host as a self-hosted GitHub Actions runner.
# Usage: scripts/register-runner.sh ROLE (app, platform, llm)
set -euo pipefail
export AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1 AWS_PAGER=""

role=${1:?usage: scripts/register-runner.sh ROLE (app, platform, llm)}
[[ "$role" =~ ^(app|platform|llm)$ ]] || { echo "role must be app, platform or llm" >&2; exit 2; }

root="$(cd "$(dirname "$0")/.." && pwd)"
repo=$(gh repo view --json nameWithOwner -q .nameWithOwner)
iid=$(terraform -chdir="$root/infra/live" output -raw "${role}_instance_id") ||
  { echo "could not read ${role}_instance_id from terraform outputs" >&2; exit 1; }

echo "fetching GitHub Actions runner registration token for $repo..."
token=$(gh api -X POST "repos/$repo/actions/runners/registration-token" -q .token) ||
  { echo "could not get registration token from GitHub (check gh auth status)" >&2; exit 1; }

echo "waiting for host lab-$role ($iid) setup to complete via SSM..."
ssm_run() {
  local cmd="$1" comment="$2"
  local params id status tries=0
  params=$(jq -cn --arg c "$cmd" '{commands: [$c]}')
  id=$(aws ssm send-command --instance-ids "$iid" --document-name AWS-RunShellScript --parameters "$params" \
    --comment "$comment" --query Command.CommandId --output text) || return 1
  status=Pending
  while [[ "$status" =~ ^(Pending|InProgress|Delayed)$ ]]; do
    ((++tries <= 90)) || { echo "timeout waiting for SSM command" >&2; return 1; }
    sleep 2
    status=$(aws ssm get-command-invocation --command-id "$id" --instance-id "$iid" --query Status --output text 2>/dev/null || echo Pending)
  done
  if [[ "$status" != Success ]]; then
    echo "SSM command failed ($status):" >&2
    aws ssm get-command-invocation --command-id "$id" --instance-id "$iid" --query StandardErrorContent --output text >&2
    return 1
  fi
  aws ssm get-command-invocation --command-id "$id" --instance-id "$iid" --query StandardOutputContent --output text
}

# Wait for /var/lib/lab-setup-done on the host
tries=0
while true; do
  ((++tries <= 60)) || { echo "host bootstrap did not finish in time" >&2; exit 1; }
  if ssm_run "test -f /var/lib/lab-setup-done" "check setup done" >/dev/null 2>&1; then
    echo "host bootstrap complete"
    break
  fi
  echo "waiting for host bootstrap... (${tries}/60)"
  sleep 5
done

RUNNER_VERSION=$(gh api repos/actions/runner/releases/latest -q .tag_name | tr -d 'v')
echo "installing and registering actions runner v$RUNNER_VERSION on lab-$role..."
install_cmd=$(cat <<EOF
set -euo pipefail
cd /opt/actions-runner
if [ -f svc.sh ]; then
  ./svc.sh stop 2>/dev/null || true
  ./svc.sh uninstall 2>/dev/null || true
fi
rm -rf /opt/actions-runner/* /opt/actions-runner/.* 2>/dev/null || true

curl -fsSL -o runner.tar.gz "https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/actions-runner-linux-arm64-${RUNNER_VERSION}.tar.gz"
tar xzf runner.tar.gz
rm -f runner.tar.gz
chown -R runner:runner /opt/actions-runner
su - runner -c "/opt/actions-runner/config.sh --url https://github.com/${repo} --token ${token} --name lab-${role} --labels lab-${role} --unattended --replace"
/opt/actions-runner/svc.sh install runner
/opt/actions-runner/svc.sh start
EOF
)

ssm_run "$install_cmd" "register actions runner" >/dev/null

echo "verifying runner status on GitHub..."
runner_status=""
for _ in {1..20}; do
  runner_status=$(gh api "repos/$repo/actions/runners" -q '.runners[] | select(.name == "lab-'$role'") | .status' 2>/dev/null || true)
  if [[ "$runner_status" == "online" ]]; then
    echo "runner lab-$role is online!"
    exit 0
  fi
  sleep 3
done

echo "runner status: ${runner_status:-not registered}"
[[ "$runner_status" == "online" ]]
