% if prefix and v:
	% if isinstance(v, tuple):
		(
		% for vv in v:
			<a href="/{{ prefix }}/{{ vv }}">{{ vv }}</a>,
		% end
		)
	% else:
		<a href="/{{ prefix }}/{{ v }}">{{ v }}</a>
	% end
% else:
	{{ v }}
% end
