% include('head', title='status')
<body>
	% if idle:
		idle
	% else:
		<table id="status-stacks">
			% for job, pid, indent, part, msg, t in tree:
				<tr><td>{{ pid }}</td>
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
</body>
