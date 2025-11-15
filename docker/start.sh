#!/bin/sh

echo "Starting SSH tunnel: $LOCAL_PORT → $SSH_HOST:$REMOTE_PORT (ssh user: $SSH_USER)"

sshpass -p "$SSH_PASS" ssh -o StrictHostKeyChecking=no -N \
  -L 0.0.0.0:${LOCAL_PORT}:127.0.0.1:${REMOTE_PORT} \
  -p ${SSH_PORT} \
  ${SSH_USER}@${SSH_HOST}
