% include('head', title=key)
<body>
	<h1>{{ key }}</h1>
	<ul class="urdlist">
	% for ts in timestamps:
		<li><a href="/urd/{{ key }}/{{ ts }}">{{ ts }}</a> <span>...</span></li>
	% end
	</ul>
<script language="javascript">
(function () {
const load = function (el) {
	const span = el.querySelector('span');
	const ts = el.querySelector('a').innerText;
	const url = '/urd/{{ key }}/' + encodeURIComponent(ts);
	fetch(url, {headers: {Accept: 'application/json'}})
	.then(res => res.json())
	.then(res => {
		span.innerText = res.caption;
	})
	.catch(error => {
		console.log('Error fetching ' + url + ':', error);
		span.innerText = '???';
		span.className = 'error';
	});
};
const observer = new IntersectionObserver(function (entries) {
	for (const entry of entries) {
		if (entry.isIntersecting) {
			const el = entry.target;
			observer.unobserve(el);
			load(el);
		}
	}
});
for (const el of document.querySelectorAll('ul.urdlist li')) {
	observer.observe(el);
}
})();
</script>
</body>
