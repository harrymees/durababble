# typed: true
# frozen_string_literal: true

require_relative "../test_helper"
require "find"
require "pathname"
require "set"

# Mirror of scripts/validate-durababble-sigils.rb, run as a Minitest test so
# every CI run catches `[DURABABBLE-*]` drift between the Alloy model and the
# Ruby implementation/specs. The script remains usable from `rake formal` for
# local Alloy workflows; this test is the canonical check in CI because it
# rides the fast `test` job rather than the long Alloy verification job.
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
