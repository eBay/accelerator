#!/bin/sh
#
# This runs several python versions under the same server.
# One method is run on each, and then the first one is used to
# verify that the data produced is what is expected.

if [ $# -lt 3 ]; then
	echo "Usage: $0 path-to-ax-repo venv-command venv-command [...]"
	echo '(Reasonable venv-commands might be "python3.5 -m venv", "virtualenv-2.7", ...)'
	echo "All venvs will be used to run test jobs."
	echo "The first venv will be used to run the server and final verification."
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

SCRIPT="`absolute "$0"`"
TEMPLATES="`dirname "$SCRIPT"`/templates"

AXREPO="`absolute "$1"`"
shift

test -d "$AXREPO" || exit 1

BASEDIR=/tmp/ax.multiple_interpreters_test.$$
mkdir $BASEDIR
cd $BASEDIR

N=0
Ns=""
for CMD in "$@"; do
	$CMD venv$N
	# Use the oldest dependencies we claim to be able to.
	./venv$N/bin/pip install "setproctitle==1.1.8" "bottle==0.12.7" "waitress==1.0"
	Ns="$Ns $N"
	N=$((N+1))
done

git clone -s "$AXREPO" ax
cd ax
for N in $Ns; do
	../venv$N/bin/python ./setup.py install
done
cd ..
rm -rf ax

./venv0/bin/ax init proj
cd proj
cat >dev/build.py <<END
from datetime import datetime
def main(urd):
	now = datetime.now()
	for n in range($N + 1):
		jid = urd.build('venv%d' % (n,), unicode_string='bl\xe5', time=now)
		urd.build('verify', source=jid, n=n, now=now)
END
echo verify >dev/methods.conf
cp "$TEMPLATES/a_verify.py" dev/

for N in $Ns; do
	echo "interpreters: venv$N ../venv$N/bin/python" >>accelerator.conf
	echo "method packages: venv$N" >>accelerator.conf
	mkdir venv$N
	touch venv$N/__init__.py
	echo venv$N venv$N >venv$N/methods.conf
	cp "$TEMPLATES/a_venvN.py" venv$N/a_venv$N.py
done


SERVER_PID=""
trap 'test -n "$SERVER_PID" && kill $SERVER_PID' exit

../venv0/bin/ax server &
SERVER_PID=$!
sleep 1
../venv0/bin/ax run
kill $SERVER_PID
SERVER_PID=""
sleep 0.2
rm -r $BASEDIR
set +xe

echo
echo OK
echo
