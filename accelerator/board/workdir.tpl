% include('head', title=name)
<body>
<h1>{{ name }}</h1>
<div id="waiting"><div class="spinner"></div></div>
<div class="filter">Filter: <input type="text" id="filter" disabled></div>
<table class="job-table">
	% for job in [name + '-LATEST'] + jobs:
		<tr><td><a href="/job/{{ job }}">{{ job }}</a></td><td>...</td><td></td></tr>
	% end
</table>
<script language="javascript">
(function () {
	const filter_change = function () {
		const want = filter.value.toLowerCase().split(/\s+/).map(v => {
			if (v) try {
				return new RegExp(v);
			} catch (e) {
				console.log('Failed to parse ' + JSON.stringify(v) + ' as regexp: ' + e.message);
			}
		}).filter(v => !!v);
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
	const filter = document.getElementById('filter');
	filter.oninput = filter_change;
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
	const all_a = document.querySelectorAll('.job-table tr td a');
	let todo = all_a.length;
	const one_done = function () {
		todo -= 1;
		if (todo == 0) {
			if (filter.value) filter_change();
			filter.disabled = false;
			document.getElementById('waiting').style.display = 'none';
		}
	}
	for (const el of all_a) {
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
			one_done();
		})
		.catch(error => {
			console.log('Error fetching ' + url + ':', error);
			td_m.innerText = '???';
			tr.className = 'error';
			one_done();
		});
	}
})();
</script>
</body>
