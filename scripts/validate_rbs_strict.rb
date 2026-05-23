# typed: true
# frozen_string_literal: true

root = File.expand_path("..", __dir__)
token = /\buntyped\b/

signature_paths = [
  *Dir.glob(File.join(root, "sig/**/*.rbs")),
  *Dir.glob(File.join(root, "sorbet/rbi/**/*.rbi")),
].sort.uniq

markdown_paths = [
  File.join(root, "README.md"),
  File.join(root, "WORKFLOW.md"),
  File.join(root, "bench/README.md"),
  *Dir.glob(File.join(root, "docs/**/*.md")),
].select { |path| File.file?(path) }.sort.uniq

violations = []

signature_paths.each do |path|
  File.foreach(path).with_index(1) do |line, number|
    next unless line.match?(token)

    violations << [path, number, line.strip]
  end
end

markdown_paths.each do |path|
  fences = []

  File.foreach(path).with_index(1) do |line, number|
    if line.start_with?("```")
      fences.empty? ? fences << true : fences.pop
      next
    end

    next if fences.empty?
    next unless token.match?(line)

    violations << [path, number, line.strip]
  end
end

if violations.empty?
  puts "Strict RBS gate passed: no untyped annotations in public signatures or documented code examples."
  exit 0
end

warn "Strict RBS gate failed: remove or justify these untyped annotations:"
violations.each do |path, number, line|
  warn "#{path.delete_prefix("#{root}/")}:#{number}: #{line}"
end
exit 1
