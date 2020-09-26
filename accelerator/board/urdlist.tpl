% include('head', title=key)
<body>
	<h1>{{ key }}</h1>
	<ul>
	% for ts in timestamps:
		<li><a href="/urd/{{ key }}/{{ ts }}">{{ ts }}</a></li>
	% end
	</ul>
</body>

