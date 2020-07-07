% include('head', title='status')
<body>
	% if idle:
		idle
	% else:
		% from accelerator.build import fmttime
		% from time import time
		% report_t = time()
		% pids = set()
		% job = part = '-'
		<table id="status-stacks">
			% for pid, indent, msg, t in status_stacks:
				<tr><td>{{ pid }}</td>
				% if indent < 0:
					% msg = msg.split('\n')
					% start = len(msg) - 1
					% while start and sum(map(bool, msg[start:])) < 5:
						% start -= 1
					% end
					<td><div class="output">
						Tail of output ({{ fmttime(report_t - t) }} ago)
						<a href="/job/{{ job }}/OUTPUT/{{ part }}">view full</a>
						<pre>{{ '\n'.join(msg[start:]) }}</pre>
					</div></td>
				% else:
					% if indent == 0:
						% job = msg.split(' ', 1)[0]
					% elif pid not in pids:
						% pids.add(pid)
						% part = ''.join(c for c in msg if c.isdigit()) or msg
					% end
					<td style="padding-left: {{ indent * 2 }}.5em">
						{{ msg }}
						({{ fmttime(report_t - t, short=True) }})
					</td>
				% end
				</tr>
			% end
		</table>
	% end
</body>
