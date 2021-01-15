#!/bin/bash
# This is for running in a manylinux2010 docker image, so /bin/bash is fine.
# docker run --rm -v /some/where:/out:rw -v /path/to/accelerator:/accelerator:ro --tmpfs /tmp:exec,size=1G quay.io/pypa/manylinux2010_x86_64 /accelerator/scripts/build_wheels.sh 20xx.xx.xx.dev1 [commit/tag/branch]
# (builds sdist if you specify a commit, needs sdist to already be in wheelhouse otherwise)

set -euo pipefail

if [ "$#" != 1 -a "$#" != "2" ]; then
	echo "Usage: $0 ACCELERATOR_BUILD_VERSION [commit/tag/branch]"
	exit 1
fi

set -x

test -d /out/wheelhouse || exit 1
test -d /accelerator/.git || exit 1
test -d /accelerator/accelerator || exit 1

case "$1" in
	20[2-9][0-9].[01][0-9].[0-3][0-9])
		ACCELERATOR_BUILD=IS_RELEASE
		;;
	20[2-9][0-9].[01][0-9].[0-3][0-9].dev[1-9])
		ACCELERATOR_BUILD=DEV
		;;
	*)
		echo "Specify a valid ACCELERATOR_BUILD_VERSION please"
		exit 1
		;;
esac
VERSION="$1"
NAME="accelerator-$(echo "$1" | sed s/\\.0/./g)"

BUILT=$'\n\nBuilt the following files:'

if [ "$#" = "1" ]; then
	test -e /out/wheelhouse/"$NAME".tar.gz || exit 1
else
	test -e /out/wheelhouse/"$NAME".tar.gz && exit 1
	cd /tmp
	rm -rf accelerator
	git clone -s /accelerator
	cd accelerator
	git checkout $2
	ACCELERATOR_BUILD_VERSION="$VERSION" ACCELERATOR_BUILD="$ACCELERATOR_BUILD" /opt/python/cp38-cp38/bin/python3 ./setup.py sdist
	cp dist/"$NAME".tar.gz /out/wheelhouse/
	BUILT="$BUILT"$'\n'"$NAME.tar.gz"
	cd ..
	rm -rf accelerator
fi


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
	BUILT="$BUILT"$'\n'"$ZLIB_PREFIX/..."
	cd ..
	rm -rf zlib-ng
fi


# Test running 2.7 and 3.5 under a 3.8 server
/opt/python/cp27-cp27mu/bin/pip install virtualenv
ACCELERATOR_BUILD_STATIC_ZLIB="$ZLIB_PREFIX/lib/libz.a" \
CPPFLAGS="-I$ZLIB_PREFIX/include" \
/accelerator/scripts/multiple_interpreters_test.sh /accelerator \
"/opt/python/cp38-cp38/bin/python -m venv" \
/opt/python/cp27-cp27mu/bin/virtualenv \
"/opt/python/cp35-cp35m/bin/python -m venv"


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
			test -e "/out/wheelhouse/${FIXED_NAME/*\//}" && continue
			rm -f "$UNFIXED_NAME" "$FIXED_NAME"
			ACCELERATOR_BUILD_STATIC_ZLIB="$ZLIB_PREFIX/lib/libz.a" \
			CPPFLAGS="-I$ZLIB_PREFIX/include" \
			"/opt/python/$V/bin/pip" wheel /out/wheelhouse/"$NAME".tar.gz --no-deps -w /tmp/wheels/
			auditwheel repair "$UNFIXED_NAME" -w /tmp/wheels/fixed/
			"/opt/python/$V/bin/pip" install "$FIXED_NAME"
			rm -rf "/tmp/ax test"
			TEST_NAME="${V/*-/}"
			if [[ "$V" =~ cp3.* ]]; then
				TEST_NAME="Ⅲ $TEST_NAME"
			else
				TEST_NAME="2 $TEST_NAME"
			fi
			"/opt/python/$V/bin/ax" init --slices "$SLICES" --name "$TEST_NAME" "/tmp/ax test"
			"/opt/python/$V/bin/ax" --config "/tmp/ax test/accelerator.conf" server &
			sleep 1
			"/opt/python/$V/bin/ax" --config "/tmp/ax test/accelerator.conf" run tests
			# The wheel passed the tests, copy it to the wheelhouse.
			cp -p "$FIXED_NAME" /out/wheelhouse/
			BUILT="$BUILT"$'\n'"${FIXED_NAME/*\//}"
			rm -rf "/tmp/ax test"
			SLICES=3 # run all other tests with the lowest (and fastest) allowed for tests
			;;
		*)
			;;
	esac
done

# Test that we can still read old job versions, from both cp27 and cp35.
# (Don't use ACCELERATOR_BUILD_STATIC_ZLIB, because these old versions don't understand it.)
/opt/python/cp38-cp38/bin/pip install /out/wheelhouse/$NAME-cp38-cp38-$AUDITWHEEL_PLAT.whl
VE=/opt/python/cp27-cp27mu/bin/virtualenv
for V in cp27-cp27mu cp35-cp35m; do
	mkdir /tmp/version_test_$V
	CPPFLAGS="-I$ZLIB_PREFIX/include" \
	LDFLAGS="-L$ZLIB_PREFIX/lib" \
	USER="DUMMY" \
	/accelerator/scripts/make_old_versions.sh /opt/python/$V/bin/python /accelerator /tmp/version_test_$V/ $VE
	PATH="/opt/python/cp38-cp38/bin:$PATH" /accelerator/scripts/check_old_versions.sh /tmp/version_test_$V/
	rm -r /tmp/version_test_$V
	VE=""
done

set +x
echo "$BUILT"
