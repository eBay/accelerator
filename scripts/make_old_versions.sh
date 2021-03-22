#!/bin/sh
#
# Generate three jobs (csvimport, dataset_type, dataset_hashpart) each on
# four older versions.
#
# There are only two different dataset versions, but they all use different
# versions of setup.json.
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
BASEDIR="$3"
VIRTUALENV="$4"

set -eux

test -x "$PYTHON" || exit 1
test -d "$AXREPO" || exit 1
test -d "$BASEDIR" || exit 1

SCRIPT="`absolute "$0"`"
TEMPLATES="`dirname "$SCRIPT"`/templates"

AXREPO="`absolute "$AXREPO"`"

SERVER_PID=""
trap 'test -n "$SERVER_PID" && kill $SERVER_PID' exit

cd $BASEDIR

cat >a.csv <<END
a,b,c
1,foo,bar
2,Foo,Bar
END

setup() {
	if [ -n "$VIRTUALENV" ]; then
		"$VIRTUALENV" -p "$PYTHON" py.$1
	else
		"$PYTHON" -m venv py.$1
	fi
	# Use the oldest dependencies we claim to be able to.
	./py.$1/bin/pip install "ujson==1.35" "setproctitle==1.1.8" "bottle==0.12.7" "waitress==1.0" "monotonic==1.0"
	git clone -s "$AXREPO" ax.$1
	cd ax.$1
	git checkout $2
	../py.$1/bin/python ./setup.py install
	cd ..
	rm -rf ax.$1
	mkdir $1
	cd $1
	# work around a python2 bug in old init versions by pre-creating slices.conf
	mkdir -p workdirs/$1
	echo 3 > workdirs/$1/$1-slices.conf
	../py.$1/bin/$3 init --force --slices 3 --name $1 $4
	case "$1" in
		v1|v2)
			HASHPART=rehash
			;;
		*)
			HASHPART=hashpart
			;;
	esac
	sed s/\\\$HASHPART/$HASHPART/g >$1/build.py <"$TEMPLATES/build_make_old_versions.py"
	../py.$1/bin/$3 $5 &
	SERVER_PID=$!
	sleep 1
	../py.$1/bin/$3 run
	kill $SERVER_PID
	SERVER_PID=""
	cd ..
}

setup v1 00feb8d8fc11bd24c59012b10ae90942e2abeb74 bd "--source .. --prefix ." daemon
setup v2 1363e5d94e08bdc16c5d3f3a6a7cb49501272f1a bd "--input .. --prefix ." daemon
setup v2b 2020.2.14.dev1 ax "--input .." server
setup v3 2020.10.1.dev1 ax "--input .." server

set +x

sleep 0.2
echo
echo OK
echo
