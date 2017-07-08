#!/bin/sh

mkdir -p /var/run/sshd
/usr/sbin/sshd
"$@" > /uwsgi.log 2>&1 &
tail -f /uwsgi.log

