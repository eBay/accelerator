% include('head', title=key)
<body>
	<h1>{{ key }}</h1>
	<table class="urd-table">
		% for thing in ('timestamp', 'user', 'build', 'caption',):
			<tr><td>{{ thing }}</td><td>{{ entry.pop(thing) }}</td></tr>
		% end
		% for thing in sorted(entry):
			% if thing not in ('joblist', 'deps',):
				<tr><td>{{ thing }}</td><td>{{ value }}</td></tr>
			% end
		% end
		<tr><td>deps</td><td>
			% for dep, depentry in sorted(entry.deps.items()):
				<a href="/urd/{{ dep }}/{{ depentry.timestamp }}">
					{{ dep }}/{{ depentry.timestamp }}
				</a>
				<ol>
					% for method, job in depentry.joblist:
						<li>{{ method }} <a href="/job/{{ job }}">{{ job }}</a></li>
					% end
				</ol>
			% end
		</td></tr>
		<tr><td>joblist</td><td>
			<ol>
				% for method, job in entry.joblist:
					<li>{{ method }} <a href="/job/{{ job }}">{{ job }}</a></li>
				% end
			</ol>
		</td></tr>
	</table>
</body>
