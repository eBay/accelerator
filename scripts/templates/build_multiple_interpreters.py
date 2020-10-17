from datetime import datetime

def main(urd):
	now = datetime.now()
	for n in range($N + 1):
		jid = urd.build('venv%d' % (n,), unicode_string='bl\xe5', time=now)
		urd.build('verify', source=jid, n=n, now=now)
