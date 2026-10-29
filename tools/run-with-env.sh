#!/usr/bin/env bash
# Executa um comando com as variáveis de um arquivo .env carregadas (o token
# nunca é impresso). Uso:
#   tools/run-with-env.sh <comando...>            # lê ./.env
#   ENV_FILE=/caminho/.env tools/run-with-env.sh <comando...>
set -euo pipefail
ENV_FILE="${ENV_FILE:-.env}"
if [ -f "$ENV_FILE" ]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi
exec "$@"
