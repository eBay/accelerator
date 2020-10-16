#!/bin/sh
#
# This checks that the jobs built by make_old_versions.sh are correctly
# readable on your currently installed accelerator.


if [ $# -ne 1 ]; then
	echo "Usage: $0 path-to-output-dir"
	echo "(populate output-dir with make_old_versions.sh)"
	exit 1
fi

SRCDIR="$1"

set -eux

test -d "$SRCDIR" || exit 1

SERVER_PID=""
trap 'test -n "$SERVER_PID" && kill $SERVER_PID' exit

BASEDIR=/tmp/ax.check_old_versions.$$
ax init --slices 3 $BASEDIR

cd $BASEDIR

echo workdirs: >>accelerator.conf
for V in v1 v2 v2b v3; do
	echo "	$V $SRCDIR/$V/workdirs/$V" >>accelerator.conf
done

cat >dev/build.py <<END
def main(urd):
	for v in ('v1', 'v2', 'v2b', 'v3'):
		version = int(v[1])
		urd.build('check', prefix=v, version=version)
END

echo check >dev/methods.conf
cat >dev/a_check.py <<END
from accelerator import Job

options = {'prefix': str, 'version': int}

def check(num, *want):
	job = Job('%s-%d' % (options.prefix, num))
	assert job.params.version == options.version
	assert job.params.versions.python_path
	if job.params.version > 2:
		assert job.params.versions.accelerator
	ds = job.dataset()
	want_lines = [len(w) for w in want]
	assert ds.lines == want_lines, '%s should have had %r lines but has %r' % (ds, want_lines, ds.lines,)
	for sliceno, want in enumerate(want):
		got = list(ds.iterate(sliceno, ('a', 'b', 'c')))
		assert got == want, '%s slice %d should have had %r but had %r' % (ds, slicen, want, got,)

def synthesis(job):
	check(0, [(b'1', b'foo', b'bar')], [(b'2', b'Foo', b'Bar')], [])
	check(1, [(1, 'foo', b'bar')], [(2, 'Foo', b'Bar')], [])
	check(2, [], [(1, 'foo', b'bar'), (2, 'Foo', b'Bar')], [])
END

ax server &
SERVER_PID=$!
sleep 1
ax run
kill $SERVER_PID
SERVER_PID=""
sleep 0.2
rm -r $BASEDIR
set +xe

echo
echo OK
echo
