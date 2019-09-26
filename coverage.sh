#!/bin/sh
for file in target/debug/async_fs-*; do [ -x "${file}" ] || continue; mkdir -p "target/cov/$(basename $file)"; kcov --exclude-pattern=/.cargo,/usr/lib --verify "target/cov/$(basename $file)" "$file"; done && bash <(curl -s https://codecov.io/bash)
