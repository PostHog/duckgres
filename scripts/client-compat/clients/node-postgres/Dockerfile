FROM node:22-bookworm-slim

WORKDIR /app
COPY clients/node-postgres/package.json .
RUN npm install --production

COPY clients/node-postgres/test_node_postgres.js .
COPY entrypoint.sh /entrypoint.sh
COPY queries.yaml /queries.yaml

ENTRYPOINT ["/entrypoint.sh", "node", "/app/test_node_postgres.js"]
