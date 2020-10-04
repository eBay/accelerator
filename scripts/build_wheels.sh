#!/bin/bash
# This is for running in a manylinux2010 docker image, so /bin/bash is fine.
# docker run --rm -v /some/where:/out:rw -v /path/to/accelerator:/accelerator:ro --tmpfs /tmp:exec,size=1G quay.io/pypa/manylinux2010_x86_64 /accelerator/scripts/build_wheels.sh /accelerator 20xx.xx.xx.dev1

set -euo pipefail

if [ "$#" != "1" ]; then
	echo "Usage: $0 ACCELERATOR_BUILD_VERSION"
	exit 1
fi

set -x

test -d /out/wheelhouse || exit 1
test -d /accelerator/.git || exit 1
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

cd /tmp
rm -rf accelerator
git clone /accelerator
cd accelerator
ACCELERATOR_BUILD_VERSION="$VERSION" /opt/python/cp38-cp38/bin/python3 ./setup.py sdist
cp dist/"$NAME".tar.gz /out/wheelhouse/
cd ..
rm -rf accelerator


MANYLINUX_VERSION="${AUDITWHEEL_PLAT/%_*}"
AUDITWHEEL_ARCH="${AUDITWHEEL_PLAT/${MANYLINUX_VERSION}_}"
ZLIB_PREFIX="/out/zlib-ng-$AUDITWHEEL_PLAT"

if [ "$MANYLINUX_VERSION" = "manylinux2010" ]; then
		# The 2010 wheels are in our case 1-compatible
		AUDITWHEEL_PLAT="manylinux1_$AUDITWHEEL_ARCH"
fi

# So you can provide a pre-built zlib-ng if you want.
if [ ! -e "$ZLIB_PREFIX/lib/libz.a" ]; then
	cd /tmp
	rm -rf zlib-ng
	git clone https://github.com/zlib-ng/zlib-ng.git
	cd zlib-ng
	git checkout 8832d7db7241194fa68509c96c092f3cf527ccce
	CFLAGS="-fPIC -fvisibility=hidden" ./configure --zlib-compat --static --prefix="$ZLIB_PREFIX"
	make install
	cd ..
	rm -rf zlib-ng
fi


# The numeric_comma test needs a locale which uses numeric comma.
localedef -i da_DK -f UTF-8 da_DK.UTF-8

cd /tmp
rm -rf /tmp/wheels
mkdir /tmp/wheels /tmp/wheels/fixed

SLICES=12 # run the first test with a few extra slices

for V in $(ls /opt/python/); do
	case "$V" in
		cp27-*|cp3[5-9]-*)
			UNFIXED_NAME="/tmp/wheels/$NAME-$V-linux_$AUDITWHEEL_ARCH.whl"
			FIXED_NAME="/tmp/wheels/fixed/$NAME-$V-$AUDITWHEEL_PLAT.whl"
			rm -f "$UNFIXED_NAME" "$FIXED_NAME"
			ACCELERATOR_BUILD_STATIC_ZLIB="$ZLIB_PREFIX/lib/libz.a" \
			CPPFLAGS="-I$ZLIB_PREFIX/include" \
			"/opt/python/$V/bin/pip" wheel /out/wheelhouse/"$NAME".tar.gz --no-deps -w /tmp/wheels/
			auditwheel repair "$UNFIXED_NAME" -w /tmp/wheels/fixed/
			"/opt/python/$V/bin/pip" install "$FIXED_NAME"
			rm -rf /tmp/axtest
			"/opt/python/$V/bin/ax" init --slices "$SLICES" --name "${V/*-/}" /tmp/axtest
			"/opt/python/$V/bin/ax" --config /tmp/axtest/accelerator.conf server &
			sleep 1
			"/opt/python/$V/bin/ax" --config /tmp/axtest/accelerator.conf run tests
			# The wheel passed the tests, copy it to the wheelhouse.
			cp -p "$FIXED_NAME" /out/wheelhouse/
			rm -rf /tmp/axtest
			SLICES=3 # run all other tests with the lowest (and fastest) allowed for tests
			;;
		*)
			;;
	esac
done
