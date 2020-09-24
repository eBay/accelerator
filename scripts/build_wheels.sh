#!/bin/bash
# This is for running in a manylinux2010 docker image, so /bin/bash is fine.
# docker run --rm -v /some/where:/out -v /path/to/accelerator:/accelerator --tmpfs /tmp quay.io/pypa/manylinux2010_x86_64 /accelerator/scripts/build_wheels.sh 20xx.xx.xx.dev1

set -euo pipefail

if [ "$#" != "1" ]; then
	echo "Usage: $0 ACCELERATOR_BUILD_VERSION"
	exit 1
fi

set -x

test -d /out/wheelhouse || exit 1
test -d /accelerator/accelerator || exit 1

case "$1" in
	20[2-9][0-9].[01][0-9].[0-3][0-9])
		;;
	20[2-9][0-9].[01][0-9].[0-3][0-9].dev[1-9])
		;;
	*)
		echo "Specify a valid ACCELERATOR_BUILD_VERSION please"
		exit 1
		;;
esac
VERSION="$1"
NAME="accelerator-$(echo "$1" | sed s/\\.0/./g)"

cd /accelerator
ACCELERATOR_BUILD_VERSION="$VERSION" /opt/python/cp38-cp38/bin/python3 ./setup.py sdist
cd /


MANYLINUX_VERSION="${AUDITWHEEL_PLAT/%_*}"
AUDITWHEEL_ARCH="${AUDITWHEEL_PLAT/${MANYLINUX_VERSION}_}"
ZLIB_PREFIX="/out/zlib-ng-$AUDITWHEEL_PLAT"
# we are (probably) in a 2010 container, but the wheel will be 1-compatible.
AUDITWHEEL_PLAT="manylinux1_$AUDITWHEEL_ARCH"

# So you can provide a pre-built zlib-ng if you want.
if [ ! -e "$ZLIB_PREFIX/lib/libz.a" ]; then
	git clone https://github.com/zlib-ng/zlib-ng.git
	cd zlib-ng
	git checkout 8832d7db7241194fa68509c96c092f3cf527ccce
	CFLAGS=-fPIC ./configure --zlib-compat --prefix="$ZLIB_PREFIX"
	make install
	cd ..
fi


# The numeric_comma test needs a locale which uses numeric comma.
localedef -i da_DK -f UTF-8 da_DK.UTF-8

rm -rf unfixed_wheels
mkdir unfixed_wheels

SLICES=12 # run the first test with a few extra slices

for V in $(ls /opt/python/); do
	case "$V" in
		cp27-*|cp3[5-9]-*)
			UNFIXED_NAME="unfixed_wheels/$NAME-$V-linux_$AUDITWHEEL_ARCH.whl"
			FIXED_NAME="/out/wheelhouse/$NAME-$V-$AUDITWHEEL_PLAT.whl"
			rm -f "$UNFIXED_NAME" "$FIXED_NAME"
			ACCELERATOR_BUILD_STATIC_ZLIB="$ZLIB_PREFIX/lib/libz.a" \
			CPPFLAGS="-I$ZLIB_PREFIX/include" \
			"/opt/python/$V/bin/pip" wheel /accelerator/dist/"$NAME".tar.gz --no-deps -w unfixed_wheels/
			auditwheel repair "$UNFIXED_NAME" -w /out/wheelhouse/
			"/opt/python/$V/bin/pip" install "$FIXED_NAME"
			rm -rf /tmp/axtest
			"/opt/python/$V/bin/ax" init --slices "$SLICES" --name "${V/*-/}" /tmp/axtest
			"/opt/python/$V/bin/ax" --config /tmp/axtest/accelerator.conf server &
			sleep 1
			USER=test "/opt/python/$V/bin/ax" --config /tmp/axtest/accelerator.conf run tests
			rm -rf /tmp/axtest
			SLICES=3 # run all other tests with the lowest (and fastest) allowed for tests
			;;
		*)
			;;
	esac
done
