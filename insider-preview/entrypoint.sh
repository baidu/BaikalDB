#!/bin/bash
# Docker Image entrypoint script

startMeta()
{
    echo "Starting baikalMeta"
    baikalMeta \
        --meta_port=8010 \
        --meta_replica_number=${META_REPLICA_NUMBER:-1} \
        --meta_server_bns=${META_SERVER_BNS:-$(hostname -i):8010}
    sleep 30

}
startStore()
{
    echo "Starting baikalStore"
    source script/init_meta_server.sh meta:8010
    source script/create_namespace.sh meta:8010
    source script/create_database.sh meta:8010
    source script/create_user.sh meta:8010
    baikalStore \
        --db_path=${DB_PATH:-/app/db} \
        --election_timeout_ms=${ELECTION_TIMEOUT_MS:-10000} \
        --raft_max_election_delay_ms=${RAFT_MAX_ELECTION_DELAY_MS:-5000} \
        --raft_election_heartbeat_factor=${RAFT_ELECTION_HEARTBEAT_FACTOR:-3} \
        --snapshot_uri=${SNAPSHOT_URI:-local://./raft_data/snapshot} \
        --meta_server_bns=${META_SERVER_BNS:-meta:8010} \
        --store_port=8110
    sleep 30
}

startDb() {
    echo "Starting baikaldb"
    source script/create_internal_table.sh meta:8010
    baikaldb \
        --meta_server_bns=${META_SERVER_BNS:-meta:8010} \
        --fetch_instance_id=${FETCH_INSTANCE_ID:-true} \
        --baikal_port=28282
}

cmd=$1
shift
case $cmd in
    meta)
        startMeta $@
        exit $?
        ;;
    store)
        startStore $@
        exit $?
        ;;
    db)
        startDb $@
        exit $?
        ;;
    *)
        echo "Unknown Command"
    exit 1
        ;;
esac
