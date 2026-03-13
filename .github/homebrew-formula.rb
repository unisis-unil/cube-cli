# Template for unisis-unil/homebrew-tools/Formula/cube.rb
# After each release, update the version, URLs, and sha256 checksums.
#
# To set up the tap:
#   1. Create repo github.com/unisis-unil/homebrew-tools
#   2. Copy this file to Formula/cube.rb
#   3. Update version/sha256 after each release
#
# Usage:
#   brew tap unisis-unil/tools
#   brew install cube

class Cube < Formula
  desc "CLI pour interroger les cubes SQLite UNISIS S3 (Statistiques en Self-Service)"
  homepage "https://github.com/unisis-unil/cube-cli"
  version "0.1.0"
  license "MIT"

  on_macos do
    on_intel do
      url "https://github.com/unisis-unil/cube-cli/releases/download/v#{version}/cube-x86_64-apple-darwin.tar.gz"
      sha256 "PLACEHOLDER"
    end

    on_arm do
      url "https://github.com/unisis-unil/cube-cli/releases/download/v#{version}/cube-aarch64-apple-darwin.tar.gz"
      sha256 "PLACEHOLDER"
    end
  end

  on_linux do
    on_intel do
      url "https://github.com/unisis-unil/cube-cli/releases/download/v#{version}/cube-x86_64-unknown-linux-musl.tar.gz"
      sha256 "PLACEHOLDER"
    end
  end

  def install
    bin.install "cube"
  end

  test do
    assert_match "cube", shell_output("#{bin}/cube --version")
  end
end
