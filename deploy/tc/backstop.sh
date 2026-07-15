#!/bin/sh
# tc TBF backstop (design doc §3.2, layer L3): kernel-level hard cap on the
# public NIC, independent of HAProxy and the agent. Rate should be ~115% of
# the node quota so it never fights the elastic ceiling (110%) and only bites
# when agent/HAProxy shaping has failed.
#
# Usage:
#   ./backstop.sh <iface> <rate_mbit>   # e.g. ./backstop.sh eth0 230
#   ./backstop.sh <iface> clear         # remove the backstop
#
# Requires CAP_NET_ADMIN (root or the rl-agent service's ambient capability).
set -eu

usage() {
    echo "usage: $0 <iface> <rate_mbit>|clear" >&2
    exit 1
}

[ $# -eq 2 ] || usage
IFACE=$1
ARG=$2
[ -n "$IFACE" ] || usage
[ -n "$ARG" ] || usage

if [ "$ARG" = "clear" ]; then
    echo "backstop: removing root qdisc on $IFACE"
    # Deleting a non-existent root qdisc is not an error for `clear`.
    tc qdisc del dev "$IFACE" root 2>/dev/null || true
    exit 0
fi

case "$ARG" in
    *[!0-9]*) echo "backstop: rate must be an integer in mbit, or 'clear'" >&2; exit 1 ;;
esac

echo "backstop: tc qdisc replace dev $IFACE root tbf rate ${ARG}mbit burst 4mb latency 50ms"
tc qdisc replace dev "$IFACE" root tbf rate "${ARG}mbit" burst 4mb latency 50ms
