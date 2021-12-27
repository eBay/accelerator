def main(urd):
	jid = urd.build(
		'csvimport',
		options=dict(filename='a.csv'),
	)
	jid = urd.build(
		'dataset_type',
		datasets=dict(source=jid),
		options=dict(column2type={'a': 'number', 'b': 'ascii'}),
	)
	urd.build(
		'dataset_$HASHPART',
		datasets=dict(source=jid),
		options=dict(hashlabel='b'),
	)
