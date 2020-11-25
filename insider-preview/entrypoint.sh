#!/bin/bash
# Docker Image entrypoint script

startMeta()
{
    echo "Starting baikalMeta"
    cd baikalMeta
    bin/baikalMeta --meta_server_bns=${META_SERVER_BNS:-$(hostname -i):8010}
    sleep 30

}
startStore()
{
    echo "Starting baikalStore"
    cd baikalStore
    source script/init_meta_server.sh meta:8010
    source script/create_namespace.sh meta:8010
    source script/create_database.sh meta:8010
    source script/create_user.sh meta:8010
    
    bin/baikalStore --meta_server_bns=${META_SERVER_BNS:-meta:8010} 
    sleep 30
}

startDb() {
    echo "Starting baikaldb"
    cd baikaldb
    source script/create_internal_table.sh meta:8010
    bin/baikaldb --meta_server_bns=${META_SERVER_BNS:-meta:8010} 
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
