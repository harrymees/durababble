# frozen_string_literal: true

require_relative "lib/durababble/version"

Gem::Specification.new do |spec|
  spec.name = "durababble"
  spec.version = Durababble::VERSION
  spec.authors = ["Shopify Engineering"]
  spec.email = ["gems@shopify.com"]

  spec.summary = "A durable Ruby workflow orchestration and RPC library."
  spec.description = spec.summary
  spec.homepage = "https://github.com/harrymees/durababble"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.4"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["rubygems_mfa_required"] = "true"

  spec.files = Dir.chdir(__dir__) do
    Dir[
      "bench/**/*",
      "docs/**/*",
      "examples/**/*",
      "lib/**/*",
      "sig/**/*",
      "LICENSE.txt",
      "README.md",
    ]
  end
  spec.require_paths = ["lib"]

  spec.add_dependency("activerecord", ">= 7.1", "< 9.0")
  spec.add_dependency("async", "~> 2.24")
  spec.add_dependency("async-http", "~> 0.90")
  spec.add_dependency("bigdecimal", "~> 3.2")
  spec.add_dependency("concurrent-ruby", "~> 1.3")
  spec.add_dependency("opentelemetry-api", "~> 1.10")
  spec.add_dependency("opentelemetry-metrics-api", "~> 0.6")
  spec.add_dependency("paquito", "~> 1.0")
  spec.add_dependency("pg", "~> 1.5")
  spec.add_dependency("trilogy", "~> 2.9")

  spec.add_development_dependency("csv", "~> 3.3")
  spec.add_development_dependency("minitest", "< 6")
  spec.add_development_dependency("mocha", "~> 2.7")
  spec.add_development_dependency("rake", "~> 13.3")
  spec.add_development_dependency("rbs", "~> 3.9")
  spec.add_development_dependency("rubocop", "~> 1.80")
  spec.add_development_dependency("rubocop-minitest", "~> 0.38")
  spec.add_development_dependency("rubocop-shopify", "~> 2.17")
  spec.add_development_dependency("rubocop-sorbet", "~> 0.10")
  spec.add_development_dependency("simplecov", "~> 0.22")
  spec.add_development_dependency("sorbet", "~> 0.5")
  spec.add_development_dependency("sqlite3", "~> 2.1")
  spec.add_development_dependency("stackprof", "~> 0.2")
end
