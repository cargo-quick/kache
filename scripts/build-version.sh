#!/bin/bash
set -euxo pipefail

RUST_TOOLCHAIN=nightly-2023-06-04

# Shamelessly stolen from https://github.com/alsuren/cargo-quickinstall/blob/main/build-version.sh

cd "$(dirname "$0")/.."

CRATE=${1?"USAGE: $0 CRATE VERSION - missing CRATE argument"}
VERSION=${2?"USAGE: $0 CRATE VERSION - missing VERSION argument"}

date

# FIXME: make a signal handler that cleans this up if we exit early.
if [ ! -d "${TEMPDIR:-}" ]; then
    TEMPDIR="$(mktemp -d)"
fi

TARGET_ARCH=$(rustc --version --verbose | sed -n 's/host: //p')

rustc "+$RUST_TOOLCHAIN" -Vv
cargo "+$RUST_TOOLCHAIN" -V

cargo "+$RUST_TOOLCHAIN" install --path=.

BINARIES=$(
    cat ~/.cargo/.crates2.json | jq -r '
        .installs | to_entries[] | select(.key|startswith("'${CRATE}' ")) | .value.bins | .[]
    ' | tr '\r' ' '
)


cd ~/.cargo/bin
for file in $BINARIES; do
    if file $file | grep ': data$'; then
        echo "something wrong with $file. Should be recognised as executable."
        exit 1
    fi
done

# Package up the binaries so that they can be untarred in ~/.cargo/bin
tar -czf "${TEMPDIR}/${CRATE}-${VERSION}-${TARGET_ARCH}.tar.gz" $BINARIES

echo "${TEMPDIR}/${CRATE}-${VERSION}-${TARGET_ARCH}.tar.gz"
