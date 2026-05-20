# frozen_string_literal: true

require_relative "lib/durababble/version"

Gem::Specification.new do |spec|
  spec.name = "durababble"
  spec.version = Durababble::VERSION
  spec.authors = ["airhorns"]
  spec.email = ["airhorns@example.invalid"]

  spec.summary = "A prototype durable Ruby execution engine backed by YugabyteDB."
  spec.description = "Durababble persists workflow and step execution state into YugabyteDB via the PostgreSQL wire protocol."
  spec.homepage = "https://example.invalid/durababble"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 4.0.0"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["rubygems_mfa_required"] = "true"

  spec.files = Dir.chdir(__dir__) do
    Dir["lib/**/*", "exe/*", "README.md", "LICENSE.txt"]
  end
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "pg", "~> 1.5"
  spec.add_development_dependency "rake", "~> 13.2"
  spec.add_development_dependency "rspec", "~> 3.13"
end
