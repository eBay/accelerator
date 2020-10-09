% include('head', title=name)
<body>
<h1>{{ name }}</h1>
<div class="filter">Filter: <input type="text" id="f-method" disabled></div>
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
		for (const el of document.querySelectorAll('.job-table tr')) {
			// innerText is '' when collapsed (at least in FF), so use innerHTML.
			const method = el.querySelector('td ~ td').innerHTML.toLowerCase();
			if (want.some(re => re.test(method))) {
				el.classList.remove('filtered');
			} else {
				el.classList.add('filtered');
			}
		}
	};
	const f_method = document.getElementById('f-method');
	f_method.oninput = filter_change;
	if (f_method.value) filter_change();
	f_method.disabled = false;
})();
</script>
</body>
