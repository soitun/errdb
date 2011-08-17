# -*- encoding: utf-8 -*-

$:.unshift File.expand_path("../lib", __FILE__)
require "errdb/version"

Gem::Specification.new do |s|
  s.name = %q{errdb}
  s.version = Errdb::VERSION

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["Ery Lee"]
  #s.autorequire = %q{errdb}
  s.description = %q{Ruby client library for Errdb(Extensibel Round Robin Database)}
  s.summary = %q{Ruby client library for Errdb}
  s.email = %q{ery.lee@gmail.com}
  s.homepage = %q{http://github.com/erylee/errdb}
  s.extra_rdoc_files = [
    "LICENSE","README.md","TODO.md","CHANGELOG.md"  
  ]
  s.files = [
    "LICENSE",
    "README.md",
    "TODO.md",
    "CHANGELOG.md",
    "lib/errdb.rb",
    "lib/errdb/version.rb",
    "examples/basic.rb"
  ]
  s.require_paths = ["lib"]
  s.rubygems_version = %q{1.5.0}
end

