Gem::Specification.new do |s|
  s.name          = 'em-stalker'
  s.version       = '0.1.0'
  s.date          = '2011-09-05'
  s.summary       = "an evented Stalker / em-jack beanstalkd library"
  s.description   = s.summary
  
  s.add_dependency 'eventmachine', ['>= 0.12.10']
  s.add_dependency 'rack-fiber_pool'

  s.add_development_dependency 'bundler', ['~> 1.0.13']
  s.add_development_dependency 'rake',    ['>= 0.8.7']
  s.add_development_dependency 'rspec',   ['~> 2.6']
  
  s.authors       = ["fonzo14"]
  s.require_paths = ["lib"]
  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.homepage      = 'http://github.com/fonzo14/em-stalker'
end