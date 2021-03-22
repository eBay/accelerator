% import json
% include('head', title=ds)
% def tdif(k, v):
	% if v:
		<tr><td>{{ k }}</td><td>{{ ! ax_link(v) }}</td></tr>
	% end
% end
<body>
	<a href="/">main</a>
	<h1>{{ ds.job }}/{{ ds.name }}</h1>
	<table>
		% tdif('job', ds.job)
		% tdif('method', ds.job.method)
		% tdif('parent', ds.parent)
		% tdif('filename', ds.filename)
		% tdif('previous', ds.previous)
		% tdif('hashlabel', ds.hashlabel)
	</table>
	<h2>columns:</h2>
	<table id="columns" class="ds-table">
	<thead>
		<tr><th>name</th><th>type</th><th>min</th><th>max</th></tr>
	</thead>
	<tbody>
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
	</tbody>
	</table>
	% cols, lines = ds.shape
	{{ cols }} columns<br>
	{{ lines }} lines {{ ds.lines }}<br>
	<h2>contents:</h2>
	<script language="javascript">
		const lines = {{ lines }};
		const columns = {{! json.dumps(sorted(ds.columns)) }};
		function toggle() {
			const checks = [];
			for (let ix = 0; ix < columns.length; ix++) {
				checks.push(document.getElementById('wantCol' + ix))
			}
			const value = !checks.every(el => el.checked);
			checks.forEach(el => el.checked = value);
		}
		function load() {
			const want_lines = parseInt(document.getElementById('nlines').value);
			if (!(want_lines > 0)) return;
			const loadEl = document.getElementById('load');
			loadEl.disabled = true;
			const enableLoad = function () {
				if (!document.querySelector('#contents .spinner')) {
					loadEl.disabled = false;
				}
			};
			const thead = document.querySelector('#contents thead');
			const tbody = document.querySelector('#contents tbody');
			const add_line = function () {
				const tr = document.createElement('TR');
				for (let col = 0; col < columns.length; col++) {
					const td = document.createElement('TD');
					td.className = 'not-loaded';
					tr.appendChild(td);
				}
				tbody.appendChild(tr);
				return tr;
			};
			const url = '/dataset/{{ ds }}?lines=' + want_lines + '&column='
			for (let col = 0; col < columns.length; col++) {
				if (!document.getElementById('wantCol' + col).checked) continue;
				const td = document.getElementById('col' + col);
				if (td.dataset.lines >= want_lines) continue;
				const spinner = document.createElement('DIV');
				spinner.className = 'spinner';
				td.appendChild(spinner);
				fetch(url + encodeURIComponent(columns[col]), {headers: {Accept: 'application/json'}})
				.then(res => res.json())
				.then(res => {
					spinner.remove();
					thead.rows[0].cells[col].className = '';
					for (let line = 0; line < res.length; line++) {
						let tr = tbody.rows[line];
						if (!tr) tr = add_line();
						tr.cells[col].className = '';
						let content = res[line];
						if (typeof content === 'object') {
							content = JSON.stringify(content);
						}
						tr.cells[col].textContent = content;
					}
					td.dataset.lines = res.length;
					enableLoad();
				})
				.catch(error => {
					console.log('Fetching ' + columns[col] + ':', error);
					spinner.remove();
					thead.rows[0].cells[col].className = 'error';
					enableLoad();
				});
			}
			enableLoad();
		}
	</script>
	<form onsubmit="event.preventDefault(); window.load();">
		<input type="submit" value="load" id="load">
		<input type="number" min="1" max="{{ lines }}" value="{{ min(lines, 1000) }}" id="nlines">
		lines
	</form>
	<input type="submit" value="all on/off" onclick="toggle();">
	<table id="contents" class="ds-table">
		<thead>
			<tr>
			% for ix, name in enumerate(sorted(ds.columns)):
				<th><label>
					<input type="checkbox" id="wantCol{{ ix }}" checked> {{ name }}
				</label></th>
			% end
			</tr>
		</thead>
		<tbody>
			<tr>
			% for ix in range(len(ds.columns)):
				<td id="col{{ ix }}" class="not-loaded" data-lines="0"></td>
			% end
			</tr>
		</tbody>
	</table>
</body>
