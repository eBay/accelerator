% include('head', title='methods')
<body>
	<h1>methods</h1>
	% for package, names in sorted(by_package.items()):
		<h2>{{ package }}</h2>
		<table class="method-table">
		% for name in names:
			% description = methods[name].description.text.split('\n')
			<tr>
				<td><a href="/method/{{ name }}">{{ name }}</a></td>
				<td>
					% for line in description:
						{{ line }}<br>
					% end
				</td>
			</tr>
		% end
		</table>
	% end
</body>
