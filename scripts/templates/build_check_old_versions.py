def main(urd):
	for v in ('ds30setup1', 'ds31setup2', 'ds31setup2b', 'ds31setup3', 'ds32setup3'):
		version = int(v.rstrip('b')[-1])
		urd.build('check', prefix=v, version=version)
