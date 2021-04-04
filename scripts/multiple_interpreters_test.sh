#!/bin/sh
#
# This runs several python versions under the same server.
# One method is run on each, and then the first one is used to
# verify that the data produced is what is expected.

if [ $# -lt 2 ]; then
	echo "Usage: $0 path-to-python-dir path-to-python-dir [...]"
	echo "(That is, paths to directories with \"python\" and \"ax\" in them, probably \"/path/to/venv/bin\".)"
	echo "All python versions will be used to run test jobs."
	echo "The first python version will be used to run the server and final verification."
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

for P in "$@"; do
	for B in python ax; do
		if [ ! -e "$P/$B" ]; then
			echo "Missing \"$B\" command in $P"
			exit 1
		fi
	done
done

BASEDIR=/tmp/ax.multiple_interpreters_test.$$
mkdir $BASEDIR
cd $BASEDIR

"$1/ax" init proj --input .
cd proj
sed s/\\\$N/$#/g >dev/build.py <"$TEMPLATES/build_multiple_interpreters.py"
echo verify >dev/methods.conf
cp "$TEMPLATES/a_verify.py" dev/

N=0
for P in "$@"; do
	echo "interpreters: p$N \"$P/python\"" >>accelerator.conf
	echo "method packages: p$N" >>accelerator.conf
	mkdir "p$N"
	touch "p$N/__init__.py"
	echo "multiple$N p$N" >p$N/methods.conf
	cp "$TEMPLATES/a_multipleN.py" p$N/a_multiple$N.py
	N=$((N+1))
done


SERVER_PID=""
trap 'test -n "$SERVER_PID" && kill $SERVER_PID' EXIT

"$1/ax" server &
SERVER_PID=$!
sleep 1
"$1/ax" run
kill $SERVER_PID
SERVER_PID=""
sleep 0.2
rm -r $BASEDIR
set +x

echo
echo OK
echo
