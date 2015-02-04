#!/usr/bin/env bash

/usr/bin/env docker --version >/dev/null 2>&1 || { echo >&2 "Docker not found"; exit 1; }
/usr/bin/env go version >/dev/null 2>&1 || { echo >&2 "Go not found"; exit 1; }

versions="5.5 5.6 5.7"
repication="row statement"
testimagename="gobinlogreplicationtest"

dockerclear() {
        /usr/bin/env docker ps | grep $testimagename > /dev/null 2>&1
        RETVAL=$?
        [ $RETVAL -eq 0 ] && docker stop $testimagename | xargs docker rm > /dev/null

        /usr/bin/env docker ps -a | grep $testimagename > /dev/null 2>&1
        RETVAL=$?
        [ $RETVAL -eq 0 ] && docker rm $testimagename > /dev/null
}

for version in $versions
do
    for rep in $repication
    do
        dockerclear

        /usr/bin/env docker build -t $testimagename "$version/$rep-based/" > /dev/null
        RETVAL=$?

        [ $RETVAL -ne 0 ] && echo "Can't build docker container" && exit 1

        /usr/bin/env docker run -p 3307:3306 \
            -d \
            -e MYSQL_ROOT_PASSWORD=admin \
            -e MYSQL_USER=admin \
            -e MYSQL_PASSWORD=admin \
            -e MYSQL_DATABASE=test \
            --name $testimagename $testimagename > /dev/null

        RETVAL=$?
        [ $RETVAL -ne 0 ] && echo "Can't run docker container" && dockerclear && exit 1

        trial=0
        for ((;;))
        do
            /usr/bin/env mysql --protocol=tcp --port=3307 --user=admin --password=admin test \
            -e "SELECT version()" > /dev/null 2>&1
            RETVAL=$?
            [ $RETVAL -eq 0 ] && break

            trial=$((trial+1))
            [ $trial -eq 20 ] && echo "Can't connect to docker mysql container" && dockerclear && exit 1
            sleep 1
        done

        echo "Test ${rep} replication MySql version ${version}"
        /usr/bin/env go test "${rep}_replication_test.go" > /dev/null

        RETVAL=$?
        [ $RETVAL -ne 0 ] && dockerclear && exit 1
        [ $RETVAL -eq 0 ] && echo -e "[ \033[0;32mOK\033[0m ]"

    done
done

dockerclear