# typed: true
# frozen_string_literal: true

require_relative "../test_helper"
require "find"
require "pathname"
require "set"

# Catches `[DURABABBLE-*]` sigil drift between the Alloy model under `formal/`
# and the Ruby implementation/tests under `lib/` and `test/`. Runs on every PR
# with the fast `test` job — the slow Alloy verifier in `rake formal` is
# gated on `formal/**` changes only, so this is the canonical drift check.
#
# To run just this test:
#
#   bundle exec ruby -Ilib -Itest test/durababble/formal_sigil_drift_test.rb
class FormalSigilDriftTest < Minitest::Test
  TAG_REGEX = /\[DURABABBLE-[A-Z0-9-]+\]/
  MODEL_DIRS = ["formal"].freeze
  RUBY_DIRS = ["lib", "test"].freeze

  ROOT = Pathname.new(File.expand_path("../..", __dir__)).freeze

  def test_every_alloy_sigil_has_a_ruby_callsite
    model_tags, model_locations = extract(find_files(MODEL_DIRS) { |file| file.extname == ".als" })
    ruby_tags, _ruby_locations = extract(find_files(RUBY_DIRS) { |file| file.extname == ".rb" })

    only_in_model = (model_tags - ruby_tags).sort
    assert_empty(
      only_in_model,
      "Alloy sigils with no Ruby callsite (Ruby refactor likely dropped the comment):\n" +
        format_locations(only_in_model, model_locations),
    )
  end

  def test_every_ruby_sigil_has_an_alloy_obligation
    model_tags, _model_locations = extract(find_files(MODEL_DIRS) { |file| file.extname == ".als" })
    ruby_tags, ruby_locations = extract(find_files(RUBY_DIRS) { |file| file.extname == ".rb" })

    only_in_ruby = (ruby_tags - model_tags).sort
    assert_empty(
      only_in_ruby,
      "Ruby sigils with no Alloy obligation (typo or stale tag):\n" +
        format_locations(only_in_ruby, ruby_locations),
    )
  end

  private

  def find_files(dirs, &filter)
    dirs.flat_map do |dir|
      full_dir = ROOT + dir
      next [] unless full_dir.directory?

      files = []
      Find.find(full_dir.to_s) do |path|
        pathname = Pathname.new(path)
        files << pathname if pathname.file? && filter.call(pathname)
      end
      files
    end
  end

  def extract(files)
    tags = Set.new
    locations = Hash.new { |hash, key| hash[key] = [] }

    files.each do |file|
      file.read.scan(TAG_REGEX).each do |tag|
        tags.add(tag)
        locations[tag] << file.relative_path_from(ROOT).to_s
      end
    end

    [tags, locations]
  end

  def format_locations(tags, locations)
    tags.map { |tag| "  #{tag} (#{locations[tag].uniq.join(", ")})" }.join("\n")
  end
end
