% include('head', title=job)
<body>
	<a href="/">main</a>
	<h1>{{ job }}/method.tar.gz</h1>
	<ul>
	% for info in members:
		<li><a href="/job/{{ job }}/method.tar.gz/{{ info.path }}">{{ info.path }}</a></li>
	% end
	</ul>
</body>
