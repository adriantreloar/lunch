#!/usr/bin/env bash
# Run all static checkers and unit tests
# Exit 0 for failed tests, for the sake of Jenkins

set -o errexit
set -o nounset
set -o pipefail

# Move to the project root directory
cd "$( dirname "${BASH_SOURCE[0]}" )/.."

rm -rf build/junit
mkdir -p build/junit

# Run flake8
uv run flake8 > build/junit/flake8.txt || true
uv run flake8_junit build/junit/flake8.txt build/junit/flake8.xml
echo

# Run black in read-only mode
set +o pipefail
uv run black -t py310 --check . 2>&1 | grep 'would reformat' > build/junit/black.txt || true
set -o pipefail
uv run flake8_junit build/junit/black.txt build/junit/black.xml.tmp
sed 's/name="flake8"/name="black"/' build/junit/black.xml.tmp > build/junit/black.xml
rm build/junit/black.xml.tmp
echo

# Run mypy
uv run mypy --install-types --non-interactive src/lunch | grep -v 'Success:' \
  | grep . | grep -v "[Collecting|Downloading].*types"  \
  | grep -v "Installing" > build/junit/mypy.txt || true
uv run flake8_junit build/junit/mypy.txt build/junit/mypy.xml.tmp
echo
sed 's/name="flake8"/name="mypy"/' build/junit/mypy.xml.tmp > build/junit/mypy.xml
rm build/junit/mypy.xml.tmp
echo

FAIL=0
cat build/junit/*.txt | awk 'END {exit NR}' || FAIL=1

# Run all unit tests and produce junit and coverage reports
rm -rf .coverage coverage.* htmlcov
uv run coverage run -m pytest src/lunch --junit-xml build/junit/pytest.xml || FAIL=1
uv run coverage combine
uv run coverage html

echo "Coverage report generated at file://$PWD/htmlcov/index.html"
cat build/junit/*.txt
if (( $FAIL == 1 ))
then
  echo "One or more tests failed"
  exit 1
fi
echo "All tests successful"
