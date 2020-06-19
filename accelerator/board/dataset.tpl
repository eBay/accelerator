% include('head', title=ds)
<body>
	<a href="/">main</a>
	<h1>{{ ds.job }}/{{ ds.name }}</h1>
	<table>
	% include('tdif', k='job', v=ds.job, prefix='job')
	% include('tdif', k='method', v=ds.job.method, prefix=None)
	% include('tdif', k='parent', v=ds.parent, prefix='dataset')
	% include('tdif', k='filename', v=ds.filename)
	% include('tdif', k='previous', v=ds.previous, prefix='dataset')
	% include('tdif', k='hashlabel', v=ds.hashlabel)
	</table>
	<h2>columns:</h2>
	<table id="columns" class="ds-table">
	<tr><th>name</th><th>type</th><th>min</th><th>max</th></tr>
	% for name, col in sorted(ds.columns.items()):
		<tr>
			<td>{{ name }}</td>
			<td>{{ col.type }}</td>
			% if col.min is not None:
				<td>{{ col.min }}</td>
				<td>{{ col.max }}</td>
			% end
		</tr>
	% end
	</table>
	% cols, lines = ds.shape
	{{ cols }} columns<br>
	{{ lines }} lines {{ ds.lines }}<br>
	<h2>contents{{ ' sample' if lines > max_lines else '' }}:</h2>
	<table id="contents" class="ds-table">
		<thead>
			<tr>
			% for name in sorted(ds.columns):
				<th>{{ name }}</th>
			% end
			</tr>
		</thead>
		<tbody>
			% from itertools import islice
			% for values in islice(ds.iterate(None), max_lines):
				<tr>
				% for value in values:
					<td>{{ value }}</td>
				% end
				</tr>
			% end
		</tbody>
	</table>
</body>
