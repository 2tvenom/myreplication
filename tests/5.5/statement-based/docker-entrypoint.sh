#!/bin/bash
set -e

DATADIR='/var/lib/mysql'

if [ "${1:0:1}" = '-' ]; then
	set -- mysqld "$@"
fi

if [ ! -d "$DATADIR/mysql" -a "${1%_safe}" = 'mysqld' ]; then
	if [ -z "$MYSQL_ROOT_PASSWORD" -a -z "$MYSQL_ALLOW_EMPTY_PASSWORD" ]; then
		echo >&2 'error: database is uninitialized and MYSQL_ROOT_PASSWORD not set'
		echo >&2 '  Did you forget to add -e MYSQL_ROOT_PASSWORD=... ?'
		exit 1
	fi

	echo 'Running mysql_install_db ...'
	mysql_install_db --basedir=/usr/local/mysql
	echo 'Finished mysql_install_db'

	# These statements _must_ be on individual lines, and _must_ end with
	# semicolons (no line breaks or comments are permitted).

	tempSqlFile='/tmp/mysql-first-time.sql'
	cat > "$tempSqlFile" <<-EOSQL
		DELETE FROM mysql.user ;
		CREATE USER 'root'@'%' IDENTIFIED BY '${MYSQL_ROOT_PASSWORD}' ;
		GRANT ALL ON *.* TO 'root'@'%' WITH GRANT OPTION ;
		DROP DATABASE IF EXISTS test ;
	EOSQL

	if [ "$MYSQL_DATABASE" ]; then
		echo "CREATE DATABASE IF NOT EXISTS \`$MYSQL_DATABASE\` ;" >> "$tempSqlFile"
	fi

	if [ "$MYSQL_USER" -a "$MYSQL_PASSWORD" ]; then
		echo "CREATE USER '$MYSQL_USER'@'%' IDENTIFIED BY '$MYSQL_PASSWORD' ;" >> "$tempSqlFile"

		if [ "$MYSQL_DATABASE" ]; then
			echo "GRANT ALL ON \`$MYSQL_DATABASE\`.* TO '$MYSQL_USER'@'%' ;" >> "$tempSqlFile"
		fi
	fi

	echo "GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO '$MYSQL_USER'@'%' ;" >> "$tempSqlFile"

	echo 'FLUSH PRIVILEGES ;' >> "$tempSqlFile"

	echo 'CREATE TABLE `test`.`new_table`(`id` int(11) NOT NULL AUTO_INCREMENT, `text_field` varchar(45) DEFAULT NULL, `num_field` int(11) NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=latin1;' >> "$tempSqlFile"

	set -- "$@" --init-file="$tempSqlFile"
fi

chown -R mysql:mysql "$DATADIR"
exec "$@"