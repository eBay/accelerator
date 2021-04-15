{{ ! template('head', title=key) }}

	<h1>{{ key }}</h1>
	<ul class="urdlist">
	% for ts, caption in timestamps:
		<li><a href="/urd/{{ key }}/{{ ts }}">{{ ts }}</a> {{ caption }}</li>
	% end
	</ul>
</body>
