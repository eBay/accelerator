% include('head', title='status')
% from datetime import datetime
<body>
	% if idle:
		<p>idle</p>
	% else:
		<table id="status-stacks">
			% for job, pid, indent, part, msg, t in tree:
				<tr><td>{{ job }}</td><td>{{ pid }}</td>
				% if indent < 0:
					<td><div class="output">
						Tail of output ({{ t }} ago)
						<a href="/job/{{ job }}/OUTPUT/{{ part }}">view full</a>
						<pre>{{ '\n'.join(msg) }}</pre>
					</div></td>
				% else:
					<td style="padding-left: {{ indent * 2 }}.5em">
						{{ msg }}
						({{ t }})
					</td>
				% end
				</tr>
			% end
		</table>
	% end
	% if get('last_error_time'):
		<p><a href="last_error?t={{ last_error_time }}">Last error at
			{{ datetime.fromtimestamp(last_error_time).replace(microsecond=0) }}
		</a></p>
	% end
</body>
