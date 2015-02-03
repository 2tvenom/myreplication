#!/usr/bin/env bash

/usr/bin/env docker --version >/dev/null 2>&1 || { echo >&2 "Docker not found"; exit 1; }

versions="5.5 5.6 5.7"
repication="row statement"
versions="5.5"
repication="row"
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
            --name $testimagename $testimagename

        RETVAL=$?
        [ $RETVAL -ne 0 ] && echo "Can't run docker container" && dockerclear && exit 1

        echo "Test ${rep} replication with MySql version ${version}"
        /usr/bin/env go test "${rep}_replication_test.go"

        RETVAL=$?
        [ $RETVAL -ne 0 ] && dockerclear && exit 1
    done
done

dockerclear