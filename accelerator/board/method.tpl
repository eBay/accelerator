{{ ! template('head', title=name) }}

	<h1>{{ data.package }}.{{ name }}</h1>
	% if data.description.text:
		% for line in data.description.text.split('\n'):
			{{ line }}<br>
		% end
	% end
	% if cfg.get('interpreters'):
		<br>Runs on &lt;{{ data.version }}&gt; {{ data.description.interpreter }}
	% end
	% for thing in ('options', 'datasets', 'jobs',):
		% if data.description.get(thing):
			<h2>{{ thing }}</h2>
			<table class="method-table">
				% for k, v in data.description[thing].items():
					<tr>
						<td>{{ k }}</td>
						% if thing == 'options':
							<td>=</td>
						% end
						<td>
							% for line in v:
								{{ line }}<br>
							% end
						</td>
					</tr>
				% end
			</table>
		% end
	% end
</body>
