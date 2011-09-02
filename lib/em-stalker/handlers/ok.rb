module EMStalker
  module Handler
    class Ok
      RESPONSE = /^OK\s+(\d+)\r\n/

      def self.handles?(response)
        if response =~ RESPONSE
          [true, $1.to_i]
        else
          false
        end
      end

      def self.handle(deferrable, response, body, conn=nil)
        return false unless response =~ RESPONSE
        bytes = $1.to_i

        deferrable.succeed(YAML.load(body))
        true
      end

      EMStalker::Connection.register_handler(EMStalker::Handler::Ok)
    end
  end
end