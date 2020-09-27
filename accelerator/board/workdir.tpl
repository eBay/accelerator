% include('head', title=name)
<body>
<h1>{{ name }}</h1>
<ul>
	<li><a href="/job/{{ name }}-LATEST">{{ name }}-LATEST</li>
	% for job in jobs:
		<li><a href="/job/{{ job }}">{{ job }}</a></li>
	% end
<ul>
</body>
