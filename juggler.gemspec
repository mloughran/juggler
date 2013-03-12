# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name        = "juggler"
  s.version     = "0.1.0"
  s.authors     = ["Martyn Loughran"]
  s.email       = ["me@mloughran.com"]
  s.homepage    = "https://github.com/mloughran/juggler"
  s.summary     = %q{Juggling background jobs with EventMachine and Beanstalkd}
  s.description = %q{Juggling background jobs with EventMachine and Beanstalkd}

  s.add_runtime_dependency "em-jack", "~> 0.1.0"
  s.add_runtime_dependency "eventmachine", "~> 1.0"

  s.add_development_dependency "em-spec"
  s.add_development_dependency "rake"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]
end
