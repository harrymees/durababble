#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."

if [[ -z "${DURABABBLE_DATABASE_URL:-}" ]]; then
  export DURABABBLE_DATABASE_URL="$(
    ruby -ruri -e '
      user = URI.encode_www_form_component(ENV.fetch("DURABABBLE_MYSQL_USERNAME", ENV.fetch("MYSQL_USER", "root")))
      password = ENV.fetch("DURABABBLE_MYSQL_PASSWORD", ENV.fetch("MYSQL_PASSWORD", nil))
      password = nil if password.to_s.empty?
      host = ENV.fetch("DURABABBLE_MYSQL_HOST", ENV.fetch("MYSQL_HOST", "127.0.0.1"))
      port = ENV.fetch("DURABABBLE_MYSQL_PORT", ENV.fetch("MYSQL_PORT", "3306"))
      database = ENV.fetch("DURABABBLE_MYSQL_DATABASE", "sidekick_server_test")
      credentials = password ? "#{user}:#{URI.encode_www_form_component(password)}" : user
      print "mysql://#{credentials}@#{host}:#{port}/#{database}"
    '
  )"
fi

exec mise exec -- bundle exec ruby examples/chat-room/server.rb
