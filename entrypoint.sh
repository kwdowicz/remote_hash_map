#!/bin/sh
set -e

# Default values
DEFAULT_NODE_LISTEN="127.0.0.1:5000"
DEFAULT_NODE_NG="127.0.0.1:6000"
DEFAULT_NG_LISTEN="127.0.0.1:6000"

# Use provided values or fall back to default values
NODE_LISTEN=${2:-$DEFAULT_NODE_LISTEN}
NODE_NG=${NG_ADDRESS:-$DEFAULT_NODE_NG}
NG_LISTEN=${2:-$DEFAULT_NG_LISTEN}

if [ "$1" = 'node' ]; then
    exec /usr/src/app/target/release/node --listen "$NODE_LISTEN" --ng "$NODE_NG"
elif [ "$1" = 'ng' ]; then
    exec /usr/src/app/target/release/ng --listen "$NG_LISTEN"
else
    exec "$@"
fi