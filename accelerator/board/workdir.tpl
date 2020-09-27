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
	for (const el of document.querySelectorAll('ul li')) {
		const url = '/job/' + encodeURIComponent(el.innerText) + '/setup.json';
		fetch(url)
		.then(res => res.json())
		.then(res => {
			el.appendChild(document.createTextNode(' ' + res.method));
		})
		.catch(error => {
			console.log('Error fetching ' + url + ':', error);
			el.appendChild(document.createTextNode('???'));
		});
	}
})();
</script>
</body>
