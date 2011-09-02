module EMStalker
  module Handler
    class Using
      RESPONSE = /^USING\s+(.*)\r\n/

      def self.handles?(response)
        response =~ RESPONSE
      end

      def self.handle(deferrable, response, body, conn=nil)
        return false unless response =~ RESPONSE

        deferrable.succeed($1)
        true
      end

      EMStalker::Connection.register_handler(EMStalker::Handler::Using)
    end
  end
end