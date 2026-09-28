#!/usr/bin/env sh
# Copies the GitHub Actions workflows into .github/workflows/ and commits them.
# (The automation token that opens PRs is not allowed to write that folder, so
#  this one-off step has to be run by a human once.)
set -eu
cd "$(dirname "$0")/.."
mkdir -p .github/workflows
cp ci/github-workflows/*.yml .github/workflows/
git add .github/workflows
git commit -m "ci: enable GitHub Actions workflows" || true
echo "Workflows installed. Now run: git push"
