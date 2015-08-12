# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'kazoo/version'

Gem::Specification.new do |spec|
  spec.name          = "kazoo-ruby"
  spec.version       = Kazoo::VERSION
  spec.authors       = ["Willem van Bergen"]
  spec.email         = ["willem@vanbergen.org"]
  spec.summary       = %q{Library to access Kafka metadata in Zookeeper}
  spec.description   = %q{Library to access Kafka metadata in Zookeeper}
  spec.homepage      = "https://github.com/wvanbergen/kazoo"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_runtime_dependency "thor", "~> 0.19.1"
  spec.add_runtime_dependency "zookeeper", "~> 1.4"

  spec.add_development_dependency "bundler", "~> 1.7"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "mocha", "~> 1.1"
  spec.add_development_dependency "minitest", "~> 5"
end
