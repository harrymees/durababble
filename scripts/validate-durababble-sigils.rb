#!/usr/bin/env ruby
# typed: true
# frozen_string_literal: true

# Validates that all [DURABABBLE-*] formal-model sigils appear in the Ruby
# implementation/specs and that Ruby-side sigils exist in the Alloy model.

require "find"
require "pathname"
require "set"

TAG_REGEX = /\[DURABABBLE-[A-Z0-9-]+\]/
MODEL_DIRS = ["formal"].freeze
RUBY_DIRS = ["lib", "test"].freeze

def find_root(start)
  dir = Pathname.new(start).expand_path

  until (dir + "durababble.gemspec").exist?
    parent = dir.parent
    raise "Could not find repository root (missing durababble.gemspec)" if parent == dir

    dir = parent
  end

  dir
end

def find_files(root, dirs)
  dirs.flat_map do |dir|
    full_dir = root + dir
    next [] unless full_dir.directory?

    files = []
    Find.find(full_dir.to_s) do |path|
      pathname = Pathname.new(path)
      files << pathname if pathname.file? && yield(pathname)
    end
    files
  end
end

def extract(files, root)
  tags = Set.new
  locations = Hash.new { |hash, key| hash[key] = [] }

  files.each do |file|
    file.read.scan(TAG_REGEX).each do |tag|
      tags.add(tag)
      locations[tag] << file.relative_path_from(root).to_s
    end
  end

  [tags, locations]
end

def difference(left, right)
  left.reject { |tag| right.include?(tag) }.sort
end

def print_tag_list(title, tags, locations)
  puts title
  tags.each do |tag|
    puts "  #{tag} (#{locations[tag].join(", ")})"
  end
end

def print_matched(tags, model_locations, ruby_locations)
  puts "Matched tags:"
  tags.each do |tag|
    puts "  #{tag}"
    puts "    Alloy: #{model_locations[tag].join(", ")}"
    puts "    Ruby:  #{ruby_locations[tag].join(", ")}"
  end
end

root = find_root(Dir.pwd)
model_files = find_files(root, MODEL_DIRS) { |file| file.extname == ".als" }
ruby_files = find_files(root, RUBY_DIRS) { |file| file.extname == ".rb" }

if model_files.empty?
  warn "No Alloy model files found under formal/"
  exit 1
end

if ruby_files.empty?
  warn "No Ruby implementation/spec files found under lib/ or test/"
  exit 1
end

model_tags, model_locations = extract(model_files, root)
ruby_tags, ruby_locations = extract(ruby_files, root)
only_in_model = difference(model_tags, ruby_tags)
only_in_ruby = difference(ruby_tags, model_tags)
in_both = model_tags.select { |tag| ruby_tags.include?(tag) }.sort

puts "Durababble sigil validation"
puts "============================"
puts "Alloy files: #{model_files.length}"
puts "Ruby files:  #{ruby_files.length}"
puts "Matched tags: #{in_both.length}"
puts "Only in Alloy: #{only_in_model.length}"
puts "Only in Ruby:  #{only_in_ruby.length}"
puts

failed = false
unless only_in_model.empty?
  failed = true
  print_tag_list("Tags in Alloy but missing from Ruby:", only_in_model, model_locations)
  puts
end

unless only_in_ruby.empty?
  failed = true
  print_tag_list("Tags in Ruby but missing from Alloy:", only_in_ruby, ruby_locations)
  puts
end

print_matched(in_both, model_locations, ruby_locations) if ARGV.include?("--verbose") || ARGV.include?("-v")

exit(failed ? 1 : 0)
