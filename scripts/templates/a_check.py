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
		assert got == want, '%s slice %d should have had %r but had %r' % (ds, sliceno, want, got,)

def synthesis(job):
	check(0, [(b'1', b'foo', b'bar')], [(b'2', b'Foo', b'Bar')], [])
	check(1, [(1, 'foo', b'bar')], [(2, 'Foo', b'Bar')], [])
	check(2, [], [(1, 'foo', b'bar'), (2, 'Foo', b'Bar')], [])
