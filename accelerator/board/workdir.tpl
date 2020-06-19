% include('head', title=name)
<h1>{{ name }}</h1>
<ul>
	<li><a href="/job/{{ name }}-LATEST">{{ name }}-LATEST</li>
	% for job in jobs:
		<li><a href="/job/{{ job }}">{{ job }}</li>
	% end
<ul>
