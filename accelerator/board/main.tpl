% include('head', title='')
<body>
	<table id="workdirs">
		% for workdir in sorted(workdirs):
			<tr>
				<td><a href="/workdir/{{ workdir }}">{{ workdir }}</a></td>
				<td><a href="/job/{{ workdir }}-LATEST">latest</a></td>
			</tr>
		% end
	</table>
	<h1 id="header">ax board: {{ project }}</h1>
	<div id="waiting"><div class="spinner"></div></div>
<script language="javascript">
(function () {
	const imageExts = new Set(['jpg', 'jpeg', 'gif', 'png', 'apng', 'svg', 'bmp', 'webp']);
	const videoExts = new Set(['mp4', 'mov', 'mpg', 'mpeg', 'mkv', 'avi', 'webm']);
	const waitingEl = document.getElementById('waiting');
	const update = function (try_num) {
		fetch('/results')
		.then(res => {
			if (res.ok) return res.json();
			throw new Error('error response');
		})
		.then(res => {
			const existing = {};
			for (const el of document.querySelectorAll('.result')) {
				existing[el.dataset.name] = el;
			};
			const items = Object.entries(res);
			if (items.length) {
				waitingEl.style.display = 'none';
			} else {
				waitingEl.style.display = 'block';
			}
			items.sort((a, b) => b[1].ts - a[1].ts);
			let prev = waitingEl;
			for (const [name, data] of items) {
				let resultEl = existing[name];
				if (resultEl) {
					delete existing[name];
					if (resultEl.dataset.ts == data.ts) {
						update_date(resultEl);
						prev = resultEl;
						continue;
					}
					while (resultEl.lastChild) resultEl.lastChild.remove();
				} else {
					resultEl = document.createElement('DIV');
				};
				const txt = text => resultEl.appendChild(document.createTextNode(text));
				const a = function (text, ...parts) {
					const a = document.createElement('A');
					a.innerText = text;
					let href = '/job'
					for (const part of parts) {
						href = href + '/' + encodeURIComponent(part);
					}
					a.href = href;
					a.target = '_blank';
					resultEl.appendChild(a);
				}
				resultEl.className = 'result';
				resultEl.dataset.name = name;
				resultEl.dataset.ts = data.ts;
				a(name, data.jobid, name);
				txt(' from ');
				a(data.jobid, data.jobid);
				txt(' (');
				const dateEl = document.createElement('SPAN');
				dateEl.className = 'date';
				resultEl.appendChild(dateEl)
				txt(')');
				update_date(resultEl);
				resultEl.appendChild(sizewrap(name, data));
				prev.after(resultEl);
				prev = resultEl;
			}
			for (const el of Object.values(existing)) {
				el.remove();
			}
			setTimeout(update, 1500);
		})
		.catch(error => {
			console.log(error);
			if (try_num === 4) {
				document.body.className = 'error';
				waitingEl.style.display = 'none';
				const header = document.getElementById('header');
				header.innerText = 'ERROR - updates stopped at ' + fmtdate();
			} else {
				waitingEl.style.display = 'block';
				setTimeout(() => update((try_num || 0) + 1), 1500);
			}
		});
	};
	const sizewrap = function (name, data) {
		if (data.size < 5000000) return load(name, data);
		const clickEl = document.createElement('DIV');
		clickEl.className = 'clickme';
		clickEl.innerText = 'Click to load ' + data.size + ' bytes';
		clickEl.onclick = function () {
			clickEl.parentNode.replaceChild(load(name, data), clickEl);
		};
		return clickEl;
	};
	const togglefull = function (event) {
		if (event.target.className) {
			event.target.className = '';
		} else {
			event.target.className = 'full';
		}
	};
	const name2ext = function (name) {
		const parts = name.split('.');
		let ext = parts.pop().toLowerCase();
		if (ext === 'gz' && parts.length > 1) {
			ext = parts.pop().toLowerCase();
		}
		return ext;
	}
	const load = function (name, data) {
		const fileUrl = '/results/' + encodeURIComponent(name) + '?ts=' + data.ts;
		const ext = name2ext(name);
		const container = document.createElement('DIV');
		const spinner = document.createElement('DIV');
		spinner.className = 'spinner';
		container.appendChild(spinner);
		const onerror = function () {
			spinner.remove();
			container.className = 'error';
			container.innerText = 'ERROR';
		};
		let fileEl;
		let stdhandling = false;
		if (imageExts.has(ext)) {
			fileEl = document.createElement('IMG');
			fileEl.onclick = togglefull;
			stdhandling = true;
		} else if (videoExts.has(ext)) {
			fileEl = document.createElement('VIDEO');
			fileEl.src = fileUrl;
			fileEl.controls = true;
			spinner.remove(); // shows a video UI immediately anyway
		} else if (ext === 'pdf') {
			fileEl = document.createElement('EMBED');
			fileEl.type = 'application/pdf';
			stdhandling = true;
		} else {
			fileEl = document.createElement('DIV');
			fileEl.className = 'textfile';
			const pre = document.createElement('PRE');
			fileEl.appendChild(pre);
			fetch(fileUrl)
			.then(res => {
				if (res.ok) return res.text();
				throw new Error('error response');
			})
			.then(res => {
				if (ext === 'html') {
					fileEl.innerHTML = res;
				} else {
					pre.innerText = res;
				}
				spinner.remove();
			})
			.catch(error => {
				console.log(error);
				onerror();
			});
		}
		if (stdhandling) {
			fileEl.onload = () => spinner.remove();
			fileEl.onerror = onerror;
			fileEl.src = fileUrl;
		}
		container.appendChild(fileEl);
		return container;
	};
	const update_date = function(el) {
		const date = new Date(el.dataset.ts * 1000);
		el.querySelector('.date').innerText = fmtdate_ago(date);
	};
	const fmtdate = function(date) {
		if (!date) date = new Date();
		return date.toISOString().substring(0, 19).replace('T', ' ') + 'Z';
	};
	const units = [['minute', 60], ['hour', 24], ['day', 365.25], ['year', 0]];
	const fmtdate_ago = function (date) {
		const now = new Date();
		let ago = (now - date) / 60000;
		for (const [unit, size] of units) {
			if (size === 0 || ago < size) {
				ago = ago.toFixed(0);
				let s = (ago == 1) ? '' : 's';
				return fmtdate(date) + ', ' + ago + ' ' + unit + s + ' ago';
			}
			ago = ago / size;
		}
	};
	update();
})();
</script>
</body>
