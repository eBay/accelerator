% include('head', title=ds)
<body>
	<a href="/">main</a>
	<h1>{{ ds }}</h1>
	<table>
	% include('tdif', k='job', v=ds.job, prefix='job')
	% include('tdif', k='method', v=ds.job.method, prefix=None)
	% include('tdif', k='parent', v=ds.parent, prefix='dataset')
	% include('tdif', k='previous', v=ds.previous, prefix='dataset')
	</table>
	<h2>columns:</h2>
	<table id="columns">
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
	{{ ds.shape[0] }} columns<br>
	{{ ds.shape[1] }} lines<br>
</body>
