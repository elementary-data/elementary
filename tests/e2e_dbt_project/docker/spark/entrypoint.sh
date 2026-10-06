#!/bin/bash

if [ -n "$WAIT_FOR" ]; then
    WAIT_FOR_TIMEOUT_SECONDS="${WAIT_FOR_TIMEOUT_SECONDS:-180}"
    IFS=';' read -a HOSTPORT_ARRAY <<< "$WAIT_FOR"
    for HOSTPORT in "${HOSTPORT_ARRAY[@]}"
    do
        WAIT_FOR_HOST=${HOSTPORT%:*}
        WAIT_FOR_PORT=${HOSTPORT#*:}
        START_TS=$(date +%s)

        echo Waiting for $WAIT_FOR_HOST to listen on $WAIT_FOR_PORT...
        while ! (exec 3<>"/dev/tcp/$WAIT_FOR_HOST/$WAIT_FOR_PORT") 2>/dev/null; do
            if [ $(( $(date +%s) - START_TS )) -ge "$WAIT_FOR_TIMEOUT_SECONDS" ]; then
                echo "Timed out waiting for $WAIT_FOR_HOST:$WAIT_FOR_PORT after ${WAIT_FOR_TIMEOUT_SECONDS}s"
                exit 1
            fi
            sleep 2
        done
    done
fi

exec spark-submit "$@"
