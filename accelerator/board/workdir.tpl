% include('head', title=name)
<body>
<h1>{{ name }}</h1>
<table class="job-table">
	% for job in [name + '-LATEST'] + jobs:
		<tr><td><a href="/job/{{ job }}">{{ job }}</a></td><td>...</td><td></td></tr>
	% end
</table>
<script language="javascript">
(function () {
	const units = [['second', 60], ['minute', 60], ['hour', 24], ['day', 0]];
	const fmttime = function (t) {
		for (const [unit, size] of units) {
			if (size === 0 || t < size * 2) {
				t = t.toFixed(2);
				let s = (t == 1) ? '' : 's';
				return t + ' ' + unit + s;
			}
			t = t / size;
		}
	};
	for (const el of document.querySelectorAll('.job-table tr td a')) {
		const url = '/job/' + encodeURIComponent(el.innerText) + '/setup.json';
		const tr = el.parentNode.parentNode;
		const td_m = el.parentNode.nextSibling;
		const td_t = td_m.nextSibling;
		fetch(url)
		.then(res => res.json())
		.then(res => {
			td_m.innerText = res.method;
			try {
				td_t.innerText = fmttime(res.exectime.total);
			} catch (e) {
				td_t.innerText = 'DID NOT FINISH'
			};
		})
		.catch(error => {
			console.log('Error fetching ' + url + ':', error);
			td_m.innerText = '???';
			tr.className = 'error';
		});
	}
})();
</script>
</body>
