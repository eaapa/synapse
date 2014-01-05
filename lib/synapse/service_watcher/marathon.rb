require "synapse/service_watcher/base"
require "synapse/log"

require 'thread'
require 'resolv'

require 'marathon'

module Synapse
  class MarathonWatcher < BaseWatcher
    include Logging
    def start
      @check_interval = @discovery['check_interval'] || 30.0
      @app_id = @discovery['app_id']

      @marathon = Marathon::Client.new(
        @discovery['host'],
        @discovery['user'],
        @discovery['pass']
      )

      watch
    end

    def ping?
      @marathon.list().success?
    end

    private
    def validate_discovery_opts
      raise ArgumentError, "invalid discovery method #{@discovery['method']}" \
        unless @discovery['method'] == 'marathon'
      raise ArgumentError, "missing or invalid app_id for service #{@name}" \
        unless @discovery['app_id']
      raise ArgumentError, "missing or invalid Marathon host for service #{@name}" \
        unless @discovery['host']
    end

    def watch
      @watcher = Thread.new do
        last_resolution = resolve_apps
        configure_backends(last_resolution)
        while true
          begin
            start = Time.now
            current_resolution = resolve_apps
            unless last_resolution == current_resolution
              last_resolution = current_resolution
              configure_backends(last_resolution)
            end

            sleep_until_next_check(start)
          rescue => e
            log.warn "Error in watcher thread: #{e.inspect}"
            log.warn e.backtrace
          end
        end
      end
    end

    def sleep_until_next_check(start_time)
      sleep_time = @check_interval - (Time.now - start_time)
      if sleep_time > 0.0
        sleep(sleep_time)
      end
    end

    def resolve_apps
      res = @marathon.endpoints(@app_id)

      if res.success?
        res.parsed_response['instances']
      else
        log.warn "Failed to list apps from Marathon: #{res}"
        []
      end
    rescue => e
      log.warn "Error while listing apps from Marathon: #{e.inspect}"
      []
    end

    def configure_backends(instances)

      new_backends = instances.map do |instance|
        {
          'name' => instance['id'],
          'host' => instance['host'],
          'port' => instance['ports'][0]
        }
      end
      
      if new_backends.empty?
        if @default_servers.empty?
          log.warn "synapse: no backends and no default servers for service #{@name};" \
            " using previous backends: #{@backends.inspect}"
        else
          log.warn "synapse: no backends for service #{@name};" \
            " using default servers: #{@default_servers.inspect}"
          @backends = @default_servers
        end
      else
        log.info "synapse: discovered #{new_backends.length} backends for service #{@name}"
        @backends = new_backends
      end
      @synapse.reconfigure!
    end
  end
end
