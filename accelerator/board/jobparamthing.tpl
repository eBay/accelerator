% if thing:
	<h3>{{ prefix }}s</h3>
	<div class="box">
		{
		<table>
		% for k, v in sorted(thing.items()):
			<tr><td>{{ k }}</td><td>=</td><td>
			% if isinstance(v, list):
				[
				% for el in v:
					% include('a_maybe', prefix=prefix, v=el)
					,
				% end
				]
			% else:
				% include('a_maybe', prefix=prefix, v=v)
			% end
			</td></tr>
		% end
		</table>
		}
	</div>
% end
