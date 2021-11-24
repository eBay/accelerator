#!/bin/sh
#
# This checks that the jobs built by make_old_versions.sh are correctly
# readable on your currently installed accelerator.


if [ $# -ne 1 ]; then
	echo "Usage: $0 path-to-output-dir"
	echo "(populate output-dir with make_old_versions.sh)"
	exit 1
fi

# Don't depend on realpath if paths are already absolute
absolute() {
	case "$1" in
		/*)
			echo "$1"
			;;
		*)
			realpath "$1"
			;;
	esac
}

set -eux

SCRIPT="$(absolute "$0")"
TEMPLATES="$(dirname "$SCRIPT")/templates"

SRCDIR="$(absolute "$1")"
test -d "$SRCDIR" || exit 1

SERVER_PID=""
trap 'test -n "$SERVER_PID" && kill $SERVER_PID' EXIT

BASEDIR=/tmp/ax.check_old_versions.$$
ax init --slices 3 $BASEDIR

cd $BASEDIR

echo workdirs: >>accelerator.conf
for V in ds30setup1 ds31setup2 ds31setup2b ds31setup3 ds32setup3; do
	echo "	$V $SRCDIR/$V" >>accelerator.conf
done

cp "$TEMPLATES/build_check_old_versions.py" dev/build.py
cp "$TEMPLATES/a_check.py" dev/
echo check >dev/methods.conf

ax server &
SERVER_PID=$!
sleep 1
ax run
kill $SERVER_PID
SERVER_PID=""
sleep 0.2
rm -r $BASEDIR
set +x

echo
echo OK
echo
