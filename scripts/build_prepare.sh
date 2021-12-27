#!/bin/bash
# This is for running in a manylinux2010 docker image, so /bin/bash is fine.
# (or manylinux2014 on non-x86 platforms)
#
# docker run -it -v /path/to/accelerator:/accelerator:ro --tmpfs /tmp:exec,size=1G quay.io/pypa/manylinux2010_x86_64:2021-02-06-c17986e /accelerator/scripts/build_prepare.sh
#
# build_wheels.sh will call this, but it's a separate script so you can run
# it once and save that docker image.

set -euo pipefail
set -x
shopt -s nullglob

test -d /accelerator/.git || exit 1
test -d /accelerator/accelerator || exit 1

if [ ! -e /opt/python/cp310-cp310/bin/python ]; then
	if [ "$AUDITWHEEL_ARCH" = "x86_64" -o "$AUDITWHEEL_ARCH" = "i686" ]; then
		if [ ! -e /opt/python/cp27-cp27mu ]; then
			echo "Old build container needs python 2.7, run in manylinux2010_$AUDITWHEEL_ARCH:2021-02-06-c17986e or earlier"
			exit 1
		fi
	fi

	if [ ! -e /opt/python/cp35-cp35m ]; then
		echo "Old build container needs python 3.5, your manylinux container must be too new"
		exit 1
	fi
fi

if [ -e /prepare/.done ]; then
	if [ "$(cat /prepare/.done)" = "3" ]; then
		exit 0
	fi
fi

rm -rf /prepare
mkdir /prepare

# The numeric_comma test needs a locale which uses numeric comma.
localedef -i da_DK -f UTF-8 da_DK.UTF-8


ZLIB_PREFIX="/prepare/zlib-ng"

cd /tmp
rm -rf zlib-ng
git clone https://github.com/zlib-ng/zlib-ng.git
cd zlib-ng
git checkout b56a2fd0b126cfe5f13e68ab9090cd4f6a773286 # 2.0.6
CFLAGS="-fPIC -fvisibility=hidden" ./configure --zlib-compat --static --prefix="$ZLIB_PREFIX"
make install
cd ..
rm -rf zlib-ng

# oldest deps we can use for <3.9, newest on >=3.9
for V in /opt/python/cp[23][5-9]-* /opt/python/cp31[0-9]-*; do
	V="${V/\/opt\/python\//}"
	case "$V" in
		cp27-*)
			/opt/python/"$V"/bin/pip install virtualenv "setproctitle==1.1.8" "bottle==0.12.7" "waitress==1.0" "configparser==3.5.0" "monotonic==1.0"
			;;
		cp3[5-8]-*)
			/opt/python/"$V"/bin/pip install "setproctitle==1.1.8" "bottle==0.12.7" "waitress==1.0"
			;;
		*)
			/opt/python/"$V"/bin/pip install setproctitle 'bottle>=0.12.7, <0.13' waitress
			;;
	esac
done


# (Don't use ACCELERATOR_BUILD_STATIC_ZLIB, because these old versions don't understand it.)
VE=/opt/python/cp27-cp27mu/bin/virtualenv
for V in cp27-cp27mu cp37-cp37m; do
	if [ -e "/opt/python/$V/bin/python" ]; then
		mkdir "/prepare/old.$V"
		CPPFLAGS="-I$ZLIB_PREFIX/include" \
		LDFLAGS="-L$ZLIB_PREFIX/lib" \
		USER="DUMMY" \
		/accelerator/scripts/make_old_versions.sh "/opt/python/$V/bin/python" /accelerator "/prepare/old.$V" $VE
	fi
	VE=""
done

echo 3 >/prepare/.done

set +x

echo
echo OK
echo
