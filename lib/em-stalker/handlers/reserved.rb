module EMStalker
  module Handler
    class Reserved
      RESPONSE = /^(RESERVED|FOUND)\s+(\d+)\s+(\d+)\r\n/

      def self.handles?(response)
        if response =~ RESPONSE
          [true, $3.to_i]
        else
          false
        end
      end

      def self.handle(deferrable, response, body, conn)
        return false unless response =~ RESPONSE
        id = $2.to_i
        bytes = $3.to_i

        job = EMStalker::Job.new(conn, id, body)
        deferrable.succeed(job)

        true
      end

      EMStalker::Connection.register_handler(EMStalker::Handler::Reserved)
    end
  end
end
