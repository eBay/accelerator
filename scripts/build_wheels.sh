#!/bin/bash
# This is for running in a manylinux2010 docker image, so /bin/bash is fine.
#
# docker run -it --rm -v /some/where:/out:rw -v /path/to/accelerator:/accelerator:ro --tmpfs /tmp:exec,size=1G quay.io/pypa/manylinux2010_x86_64:2021-02-06-c17986e /accelerator/scripts/build_wheels.sh 20xx.xx.xx.dev1 [commit/tag/branch]
#
# or preferably:
# docker run --rm --network none -v /some/where:/out:rw -v /path/to/accelerator:/accelerator:ro --tmpfs /tmp:exec,size=1G YOUR_DOCKER_IMAGE_YOU_HAVE_RUN_build_prepare.sh /accelerator/scripts/build_wheels.sh 20xx.xx.xx.dev1 [commit/tag/branch]
#
# builds sdist if you specify a commit, needs sdist to already be in wheelhouse otherwise
# if you run it in an image where you have already run build_prepare.sh you can run it with --network none

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
NAME="accelerator-${VERSION//.0/.}"

BUILT=()

/accelerator/scripts/build_prepare.sh


SDIST="/out/wheelhouse/$NAME.tar.gz"
if [ "$#" = "1" ]; then
	test -e "$SDIST" || exit 1
	BUILT_SDIST=""
else
	test -e "$SDIST" && exit 1
	cd /tmp
	rm -rf accelerator
	git clone -s /accelerator
	cd accelerator
	git checkout "$2"
	ACCELERATOR_BUILD_VERSION="$VERSION" ACCELERATOR_BUILD="$ACCELERATOR_BUILD" /opt/python/cp38-cp38/bin/python3 ./setup.py sdist
	cp -p "dist/$NAME.tar.gz" /tmp/
	SDIST="/tmp/$NAME.tar.gz"
	BUILT_SDIST="$SDIST"
	cd ..
	rm -rf accelerator
fi


MANYLINUX_VERSION="${AUDITWHEEL_PLAT/%_*}"
AUDITWHEEL_ARCH="${AUDITWHEEL_PLAT/${MANYLINUX_VERSION}_}"
ZLIB_PREFIX="/prepare/zlib-ng"

if [ "$MANYLINUX_VERSION" = "manylinux2010" ]; then
		# The 2010 wheels are in our case 1-compatible
		AUDITWHEEL_PLAT="manylinux1_$AUDITWHEEL_ARCH"
fi


# The numeric_comma test needs a locale which uses numeric comma.
localedef -i da_DK -f UTF-8 da_DK.UTF-8

cd /tmp
rm -rf /tmp/wheels
mkdir /tmp/wheels /tmp/wheels/fixed

SLICES=12 # run the first test with a few extra slices

for V in /opt/python/cp[23][5-9]-*; do
	V="${V/\/opt\/python\//}"
	case "$V" in
		cp27-*|cp3[5-9]-*)
			UNFIXED_NAME="/tmp/wheels/$NAME-$V-linux_$AUDITWHEEL_ARCH.whl"
			FIXED_NAME="/tmp/wheels/fixed/$NAME-$V-$AUDITWHEEL_PLAT.whl"
			test -e "/out/wheelhouse/${FIXED_NAME/*\//}" && continue
			rm -f "$UNFIXED_NAME" "$FIXED_NAME"
			ACCELERATOR_BUILD_STATIC_ZLIB="$ZLIB_PREFIX/lib/libz.a" \
			CPPFLAGS="-I$ZLIB_PREFIX/include" \
			"/opt/python/$V/bin/pip" wheel "$SDIST" --no-deps -w /tmp/wheels/
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
			rm -rf "/tmp/ax test"
			# verify that we can still read old datasets
			for SRCDIR in /prepare/old.cp27-cp27mu /prepare/old.cp37-cp37m; do
				PATH="/opt/python/$V/bin:$PATH" /accelerator/scripts/check_old_versions.sh "$SRCDIR"
			done
			# The wheel passed the tests, copy it to the wheelhouse (later).
			BUILT+=("$FIXED_NAME")
			SLICES=3 # run all other tests with the lowest (and fastest) allowed for tests
			;;
		*)
			;;
	esac
done


# Test running 2.7 and 3.5 under a 3.8 server
/accelerator/scripts/multiple_interpreters_test.sh \
	/opt/python/cp38-cp38/bin \
	/opt/python/cp27-cp27mu/bin \
	/opt/python/cp35-cp35m/bin

# Test running 3.6 and 3.9 under a 2.7 server
/accelerator/scripts/multiple_interpreters_test.sh \
	/opt/python/cp27-cp27m/bin \
	/opt/python/cp36-cp36m/bin \
	/opt/python/cp39-cp39/bin


# finally copy everything to /out/wheelhouse
for N in "${BUILT[@]}"; do
	cp -p "$N" /out/wheelhouse/
done
if [ -n "$BUILT_SDIST" ]; then
	cp -p "$BUILT_SDIST" /out/wheelhouse/
	BUILT+=("$BUILT_SDIST")
fi


set +x

echo
echo
echo "Built the following files:"
for N in "${BUILT[@]}"; do
	echo "${N/*\//}"
done
