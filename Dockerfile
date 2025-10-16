# docker build -t custom-mysql:5.7-debian-ram .

FROM circleci/mysql:5.7-debian-ram

# Create startup script
RUN echo '#!/bin/bash' > /usr/local/bin/start-mysql.sh \
 && echo '' >> /usr/local/bin/start-mysql.sh \
 && echo '# Initialize datadir if empty' >> /usr/local/bin/start-mysql.sh \
 && echo 'if [ ! -d /var/lib/mysql/mysql ]; then' >> /usr/local/bin/start-mysql.sh \
 && echo '    echo "[INFO] Initializing MySQL data directory..."' >> /usr/local/bin/start-mysql.sh \
 && echo '    mysqld --initialize-insecure --user=mysql' >> /usr/local/bin/start-mysql.sh \
 && echo 'fi' >> /usr/local/bin/start-mysql.sh \
 && echo '' >> /usr/local/bin/start-mysql.sh \
 && echo '# Start mysqld as mysql user' >> /usr/local/bin/start-mysql.sh \
 && echo 'gosu mysql mysqld &' >> /usr/local/bin/start-mysql.sh \
 && echo 'tail -f /dev/null' >> /usr/local/bin/start-mysql.sh \
 && chmod +x /usr/local/bin/start-mysql.sh

# Use the wrapper as entrypoint
ENTRYPOINT ["/usr/local/bin/start-mysql.sh"]

# Expose MySQL port
EXPOSE 3306