def main(urd):
	for v in ('v1', 'v2', 'v2b', 'v3'):
		version = int(v[1])
		urd.build('check', prefix=v, version=version)
