require 'eventmachine'

module EMStalker
  class BeanstalkConnection < EM::Connection
    attr_accessor :client

    def connection_completed
      @client.connected
    end

    def receive_data(data)
      @client.received(data)
    end

    def send(cmd, *args)
      send_data(command(cmd,*args))
    end

    def send_with_data(cmd, data, *args)
      send_data(command_with_data(cmd, data, *args))
    end
    
    def command(cmd, *args)
      cmd = cmd.to_s
      cmd << " #{args.join(" ")}" unless args.length == 0
      cmd << "\r\n"
      cmd
    end
    
    def command_with_data(cmd, data, *args)
      "#{cmd.to_s} #{args.join(" ")}\r\n#{data}\r\n"
    end

    def unbind
      @client.disconnected
    end
  end
end