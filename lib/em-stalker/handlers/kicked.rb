module EMStalker
  module Handler
    class Kicked
      RESPONSE = /^KICKED\s+(\d+)\r\n/

      def self.handles?(response)
        response =~ RESPONSE
      end

      def self.handle(deferrable, response, body, conn=nil)
        return false unless response =~ RESPONSE

        deferrable.succeed($1.to_i)
        true
      end

      EMStalker::Connection.register_handler(EMStalker::Handler::Kicked)
    end
  end
end