% include('head', title=name)
<body class="workdir">
<h1>{{ name }}</h1>
<div class="filter">
	<h1>Filter</h1>
	<table>
		<tr><td>Method</td><td><input type="text" id="f-method" disabled></td></tr>
		<tr>
			<td>State</td>
			<td>
				% for state in ('current', 'old', 'unfinished'):
				<input id="f-{{ state }}" value="{{ state }}" type="checkbox" checked disabled>
				<label for="f-{{ state }}"> {{ state }}</label><br>
				% end
			</td>
		</tr>
	</table>
</div>
<table class="job-table">
	% for job, data in jobs.items():
		<tr class="{{ data.klass }}">
			<td><a href="/job/{{ job }}">{{ job }}</a></td>
			<td>{{ data.method }}</td><td>{{ data.totaltime or 'DID NOT FINISH' }}</td>
		</tr>
	% end
</table>
<script language="javascript">
(function () {
	const filter_change = function () {
		const want = f_method.value.toLowerCase().split(/\s+/).filter(v => !!v).map(v => {
			try {
				return new RegExp(v);
			} catch (e) {
				console.log('Failed to parse ' + JSON.stringify(v) + ' as regexp: ' + e.message);
			}
		});
		if (want.includes(undefined)) {
			f_method.className = 'error';
			return;
		}
		f_method.className = '';
		if (want.length == 0) want.push(/./); // nothing -> all
		const classes = Array.from(
			document.querySelectorAll('.filter input[type="checkbox"]:checked'),
			el => el.value,
		);
		const filtered = function(el) {
			if (!classes.some(cls => el.classList.contains(cls))) return true;
			// innerText is '' when collapsed (at least in FF), so use innerHTML.
			const method = el.querySelector('td ~ td').innerHTML.toLowerCase();
			return !want.some(re => re.test(method));
		}
		for (const el of document.querySelectorAll('.job-table tr')) {
			if (filtered(el)) {
				el.classList.add('filtered');
			} else {
				el.classList.remove('filtered');
			}
		}
	};
	const f_method = document.getElementById('f-method');
	filter_change();
	for (const el of document.querySelectorAll('.filter input[type="checkbox"]')) {
		el.oninput = filter_change;
		el.disabled = false;
	}
	f_method.oninput = filter_change;
	f_method.disabled = false;
})();
</script>
</body>
