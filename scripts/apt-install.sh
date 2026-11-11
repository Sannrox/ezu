#!/bin/sh
# Install Debian packages for CI, retrying runner DNS and mirror failures.
#
# Usage: scripts/apt-install.sh PACKAGE...
#
# Runs `apt-get update` and `apt-get install -y` together up to
# APT_INSTALL_ATTEMPTS times (default 4), sleeping 15, 30, then 60 seconds
# between tries. `apt-get update` only warns when a mirror does not resolve,
# so a failed install retries the update too. Exits with apt's last error
# once attempts run out, so a real outage stays red. Uses sudo unless root.
# POSIX sh, so it also runs in minimal containers.
set -eu

[ "$#" -gt 0 ] || {
  echo "usage: apt-install.sh PACKAGE..." >&2
  exit 2
}

sudo=""
[ "$(id -u)" -eq 0 ] || sudo="sudo"
attempts="${APT_INSTALL_ATTEMPTS:-4}"
delay=15
try=1
while :; do
  if $sudo apt-get -o Acquire::Retries=3 update -qq &&
    $sudo env DEBIAN_FRONTEND=noninteractive apt-get -o Acquire::Retries=3 install -y -qq "$@"; then
    exit 0
  fi
  if [ "$try" -ge "$attempts" ]; then
    echo "apt-install: giving up on $* after $try attempts" >&2
    exit 1
  fi
  echo "apt-install: attempt $try failed; retrying in ${delay}s" >&2
  sleep "$delay"
  try=$((try + 1))
  delay=$((delay * 2))
done
