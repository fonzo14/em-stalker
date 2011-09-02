module EMStalker
  module Handler
    class Released
      RESPONSE = /^RELEASED\r\n/

      def self.handles?(response)
        response =~ RESPONSE
      end

      def self.handle(deferrable, response, body, conn=nil)
        return false unless response =~ RESPONSE

        deferrable.succeed
        true
      end

      EMStalker::Connection.register_handler(EMStalker::Handler::Released)
    end
  end
end