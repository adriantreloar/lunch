#!/usr/bin/env bash
# Run all static checkers and unit tests
# Exit 0 for failed tests, for the sake of Jenkins

set -o errexit
set -o nounset
set -o pipefail

# Move to the project root directory
cd "$( dirname "${BASH_SOURCE[0]}" )/.."

rm -rf build/junit
mkdir build/junit

# Avoid accidentally loading installed modules
export PYTHONPATH=$PWD

# Run flake8
flake8 > build/junit/flake8.txt || true
flake8_junit build/junit/flake8.txt build/junit/flake8.xml
echo

# Run black in read-only mode
set +o pipefail
black -t py38 --check . 2>&1 | grep 'would reformat' > build/junit/black.txt || true
set -o pipefail
flake8_junit build/junit/black.txt build/junit/black.xml.tmp
sed 's/name="flake8"/name="black"/' build/junit/black.xml.tmp > build/junit/black.xml
rm build/junit/black.xml.tmp
echo

# Run mypy
mypy --install-types --non-interactive lunch | grep -v 'Success:' \
  | grep . | grep -v "[Collecting|Downloading].*types"  \
  | grep -v "Installing" > build/junit/mypy.txt || true
flake8_junit build/junit/mypy.txt build/junit/mypy.xml.tmp
echo
sed 's/name="flake8"/name="mypy"/' build/junit/mypy.xml.tmp > build/junit/mypy.xml
rm build/junit/mypy.xml.tmp
echo

FAIL=0
cat build/junit/*.txt | awk 'END {exit NR}' || FAIL=1

# Run all unit tests and produce junit and coverage reports
rm -rf .coverage ,coverage.* htmlcov
coverage run -m pytest lunch --junit-xml build/junit/pytest.xml || FAIL=1
coverage combine
coverage html

echo "Coverage report generated at file://$PWD/htmlcov/index.html"
cat buil/junit*.txt
if (( $FAIL == 1))
then
  echo "One or more tests failed"
  exit 1
fi
echo "All tests successful"