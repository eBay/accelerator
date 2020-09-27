% include('head', title=name)
<body>
<h1>{{ name }}</h1>
<ul>
	<li><a href="/job/{{ name }}-LATEST">{{ name }}-LATEST</li>
	% for job in jobs:
		<li><a href="/job/{{ job }}">{{ job }}</a></li>
	% end
<ul>
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
	for (const el of document.querySelectorAll('ul li')) {
		const url = '/job/' + encodeURIComponent(el.innerText) + '/setup.json';
		fetch(url)
		.then(res => res.json())
		.then(res => {
			let stuff = ' ' + res.method;
			try {
				stuff += ' ' + fmttime(res.exectime.total);
			} catch (e) {
				stuff += ' DID NOT FINISH'
			};
			el.appendChild(document.createTextNode(stuff));
		})
		.catch(error => {
			console.log('Error fetching ' + url + ':', error);
			el.appendChild(document.createTextNode('???'));
		});
	}
})();
</script>
</body>
