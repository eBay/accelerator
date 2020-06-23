% include('head', title=job)
<body>
	<a href="/">main</a>
	<h1>{{ job }}</h1>
	<h2>setup</h2>
	<div class="box">
		{{ params.package }}.{{ params.method }}<br>
		<a href="/job/{{ job }}/method.tar.gz/">Source</a>
		% if params.options:
			<h3>options</h3>
			<div class="box">
				{
				<table>
				% for k, v in sorted(params.options.items()):
					<tr><td>{{ k }}</td><td>=</td><td>{{ repr(v) }}</td></tr>
				% end
				</table>
				}
			</div>
		%end
		% include('jobparamthing', thing=params.datasets, prefix='dataset')
		% include('jobparamthing', thing=params.jobs, prefix='job')
	</div>
	% if datasets:
		<h2>datasets</h2>
		<div class="box">
			<ul>
				% for ds in datasets:
					<li><a href="/dataset/{{ ds }}">{{ ds }}</a> {{ '%d columns, %d lines' % ds.shape }}</li>
				% end
			</ul>
		</div>
	% end
	% if subjobs:
		<h2>subjobs</h2>
		<div class="box">
			<ul>
				% for j in subjobs:
					<li><a href="/job/{{ j }}">{{ j }}</a> {{ j.method }}</li>
				% end
			</ul>
		</div>
	% end
	% if files:
		<h2>files</h2>
		<div class="box">
			<ul>
				% for fn in sorted(files):
					<li><a target="_blank" href="/job/{{ job }}/{{ fn }}">{{ fn }}</a></li>
				% end
			</ul>
		</div>
	% end
	% if output:
		<h2>output</h2>
		<div class="box" id="output">
			<div class="spinner"></div>
		</div>
		<script language="javascript">
			(function () {
				const output = document.getElementById('output');
				const spinner = output.querySelector('.spinner');
				const create = function (name, displayname) {
					const el = document.createElement('DIV');
					el.id = displayname;
					el.className = 'spinner';
					output.appendChild(el);
					const h3 = document.createElement('H3');
					h3.innerText = displayname;
					const pre = document.createElement('PRE');
					fetch('/job/{{ job }}/OUTPUT/' + name)
					.then(res => {
						if (res.status == 404) {
							el.remove();
						} else if (!res.ok) {
							throw new Error(displayname + ' got ' + res.status)
						} else {
							return res.text();
						}
					})
					.then(text => {
						el.appendChild(h3);
						pre.innerText = text;
						el.appendChild(pre);
						el.className = '';
					})
					.catch(error => {
						console.log(error);
						el.appendChild(h3);
						pre.innerText = 'FETCH ERROR';
						el.appendChild(pre);
						el.className = 'error';
					});
				};
				create('prepare', 'prepare');
				for (let sliceno = 0; sliceno < {{ job.params.slices }}; sliceno++) {
					create(sliceno, 'analysis-' + sliceno);
				}
				create('synthesis', 'synthesis');
				spinner.remove();
			})();
		</script>
	% end
</body>
