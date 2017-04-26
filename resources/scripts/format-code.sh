#!/bin/bash
# Auto format changed java files using google-java-format.
# TODO: make this a pre commit hook, putting it in $repo/.git/hooks

cd `git rev-parse --show-toplevel`

java -jar resources/scripts/google-java-format-1.3-all-deps.jar --replace `find . -name "*.java" -type f -printf " %p"`




