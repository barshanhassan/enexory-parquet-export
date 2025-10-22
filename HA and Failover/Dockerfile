# For testing purposes only
# docker build -t custom-mysql:5.7-debian-ram .

FROM circleci/mysql:5.7-debian-ram

RUN echo '#!/bin/bash' > /usr/local/bin/start-mysql.sh \
 && echo 'if [ ! -d /var/lib/mysql/mysql ]; then' >> /usr/local/bin/start-mysql.sh \
 && echo '    echo "[INFO] Initializing MySQL..."' >> /usr/local/bin/start-mysql.sh \
 && echo '    mysqld --initialize-insecure --user=mysql' >> /usr/local/bin/start-mysql.sh \
 && echo 'fi' >> /usr/local/bin/start-mysql.sh \
 && echo 'gosu mysql mysqld &' >> /usr/local/bin/start-mysql.sh \
 && echo 'tail -f /dev/null' >> /usr/local/bin/start-mysql.sh \
 && chmod +x /usr/local/bin/start-mysql.sh

ENTRYPOINT ["/usr/local/bin/start-mysql.sh"]

EXPOSE 3306