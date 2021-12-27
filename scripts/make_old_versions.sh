#!/bin/sh
#
# Generate three jobs (csvimport, dataset_type, dataset_hashpart) each on
# five older versions, with increasing Dataset and setup.json versions.
#
# Doesn't work past Python 3.7 because of bugs in older accelerator versions.

if [ $# -lt 3 -o $# -gt 4 ]; then
	echo "Usage: $0 path-to-python path-to-ax-repo path-to-output-dir [virtualenv]"
	echo "python must be 2.7 or 3.5 - 3.7"
	echo "if \$PYTHON -m venv doesn't work you need to specify a virtualenv command"
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

PYTHON="$1"
AXREPO="$2"
OUTDIR="$3"
VIRTUALENV="$4"

set -eux

test -x "$PYTHON" || exit 1
test -d "$AXREPO" || exit 1
test -d "$OUTDIR" || exit 1

SCRIPT="$(absolute "$0")"
TEMPLATES="$(dirname "$SCRIPT")/templates"

AXREPO="$(absolute "$AXREPO")"

SERVER_PID=""
trap 'test -n "$SERVER_PID" && kill $SERVER_PID' EXIT

BASEDIR=/tmp/make_old_versions.$$
mkdir $BASEDIR
cd $BASEDIR

cat >a.csv <<END
a,b
111,fits in 1 byte
-2,also fits in 1 byte
333,needs 3 bytes
44444,fits in 5 bytes
55555555555,fits in 9 bytes
666666666666666666666,fits in 10 bytes
END

setup() {
	if [ -n "$VIRTUALENV" ]; then
		"$VIRTUALENV" -p "$PYTHON" --system-site-packages "py.$1"
	else
		"$PYTHON" -m venv --system-site-packages "py.$1"
	fi
	# Use the oldest dependencies we claim to be able to.
	"./py.$1/bin/pip" install "ujson==1.35" "setproctitle==1.1.8" "bottle==0.12.7" "waitress==1.0"
	git clone -s "$AXREPO" "ax.$1"
	cd "ax.$1"
	git checkout "$2"
	"../py.$1/bin/python" ./setup.py install
	cd ..
	rm -rf "ax.$1"
	rm -rf "$OUTDIR/$1"
	mkdir "$OUTDIR/$1"
	mkdir "$1"
	cd "$1"
	mkdir workdirs
	ln -s "$OUTDIR/$1" workdirs/
	# work around a python2 bug in old init versions by pre-creating slices.conf
	echo 3 > "workdirs/$1/$1-slices.conf"
	"../py.$1/bin/$3" init --force --slices 3 --name "$1" $4
	case "$1" in
		ds30setup1|ds31setup2)
			HASHPART=rehash
			;;
		*)
			HASHPART=hashpart
			;;
	esac
	sed "s/\\\$HASHPART/$HASHPART/g" >"$1/build.py" <"$TEMPLATES/build_make_old_versions.py"
	"../py.$1/bin/$3" "$5" &
	SERVER_PID=$!
	sleep 1
	"../py.$1/bin/$3" run
	kill $SERVER_PID
	SERVER_PID=""
	cd ..
}

setup ds30setup1 00feb8d8fc11bd24c59012b10ae90942e2abeb74 bd "--source .. --prefix ." daemon
setup ds31setup2 1363e5d94e08bdc16c5d3f3a6a7cb49501272f1a bd "--input .. --prefix ." daemon
setup ds31setup2b 2020.2.14.dev1 ax "--input .." server
setup ds31setup3 2020.10.1.dev1 ax "--input .." server
setup ds32setup3 2021.10.28.dev1 ax "--input .." server

sleep 0.2
rm -rf $BASEDIR

set +x

echo
echo OK
echo
