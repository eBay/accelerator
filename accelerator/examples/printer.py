# This isn't intended as an example, it's just here to simplify all the
# output formating the examples do.

from accelerator import colour

class Printer:
	def __init__(self, indent=0):
		self._indent = indent
		self._last_indent = 0
		self._unindent = []

	def _prefix(self, txt):
		for c in txt:
			if not c.isspace():
				break
			yield c
		yield None

	def _print(self, a, indent, *attrs):
		txt = ' '.join(str(v) for v in a)
		prefixes = []
		lines = txt.lstrip('\n').rstrip().split('\n')
		for line in lines:
			if line.strip():
				prefixes.append(list(self._prefix(line)))
		prefix_len = 0
		if prefixes:
			prefixes[0].pop()
			while len(prefixes[0]) > prefix_len and len(set(p[prefix_len] for p in prefixes)) == 1:
				prefix_len += 1
		lines = [line[prefix_len:].rstrip() for line in lines]
		self._last_indent = max(indent + self._indent, 0)
		indent = ' ' * self._last_indent
		lines = [indent + line if line else '' for line in lines]
		txt = '\n'.join(lines)
		if attrs and txt:
			txt = colour(txt, *attrs)
		print(txt)
		return self

	def __call__(self, *a):
		return self._print(a, 0, 'brightblue')

	def header(self, *a):
		return self._print(a, 0, 'bold', 'brightblue')

	def command(self, *a):
		return self._print(a, 2, 'bold')

	def output(self, *a):
		return self._print(a, 2)

	def plain(self, *a):
		return self._print(a, 0)

	def source(self, filename):
		return self._print(('Source: ' + colour.bold(filename),), -1000)

	def __enter__(self):
		self._unindent.append(self._indent)
		self._indent = self._last_indent + 2
		return self

	def __exit__(self, e_type, e_value, e_tb):
		self._indent = self._unindent.pop()

prt = Printer()
