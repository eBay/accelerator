{{ ! template('head', title='urd for ' + project) }}

	<h1>urd for {{ project }}</h1>
	<ul class="urdlist">
	% for thing in lists:
		<li>
			<a href="/urd/{{ thing }}">{{ thing }}</a>
			<a href="/urd/{{ thing }}/first">first</a>
			<a href="/urd/{{ thing }}/latest">latest</a>
		</li>
	% end
	</ul>
</body>
