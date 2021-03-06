require 'spec_helper'

if RUBY_VERSION > '1.9'
  require 'fiber'

  describe EMStalker::Connection do    
    it "should process live messages" do
      EM.run do
        EM.add_timer(5) { EM.stop }

        Fiber.new do
          bean = EMStalker::Connection.new
          bean.fiber!

          bean.watch("toto")
  
          bean.enqueue("toto","foo")

          job = bean.reserve()
          job.tube.should eq "toto"
          job.body.should eq "foo"
          job.delete

          EM.stop
        end.resume
        
      end
    end
    it "should process each job" do
      EM.run do
        EM.add_timer(5) { EM.stop }
        
        job_body = ''
        
        f = Fiber.new do
          bean = EMStalker::Connection.new
          bean.fiber!
        
          bean.watch("toto")
        
          bean.enqueue("toto","hello")
          bean.enqueue("toto","bonjour")   
          
          mock = double()
          mock.should_receive(:tube).with("toto")
          mock.should_receive(:foo).with("hello")
          mock.should_receive(:tube).with("toto")
          mock.should_receive(:foo).with("bonjour")
          
          bean.each_job(:timeout => 0) do |job|
            mock.tube(job.tube)
            mock.foo(job.body)
            job_body = job.body
            job.delete
          end
          
        end
        
        f.resume
        
        EM.add_timer(1) { EM.stop unless f.alive?; job_body.should eq "bonjour" unless f.alive? }
        
      end
    end
  end
end
