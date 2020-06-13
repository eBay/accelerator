<!DOCTYPE html>
<head>
	<title>ax board{{ title and ' - ' + title }}</title>
	<style>
		:root {
			--bg0: #fffff9;
			--bg1: #fafff9;
			--bg2: #fffbf9;
			--bg0dark: #f8f4f1;
			--fg0: #222;
			--border0: #ecece8;
			--border1: #ccb;
			--max-height: 300px;
		}

		a:link { color: #11F; }
		a:visited { color: #529; }
		a:active { color: #C16; }
		body {
			background: var(--bg0);
			color: var(--fg0);
		}
		a[href="/"] {
			float: right;
		}
		.error {
			background: red;
		}

		.textfile {
			width: 100%;
			max-height: var(--max-height);
			overflow: auto;
		}
		.result {
			border-top: 2px solid #aad;
			padding-top: 10px;
			margin-bottom: 22px;
		}
		.clickme {
			text-align: center;
			font-size: 120%;
			padding: 15px;
			background: #fbb;
		}
		video, embed {
			display: block;
			max-height: var(--max-height);
			width: 100%;
		}
		embed {
			height: var(--max-height);
		}
		img {
			display: block;
			max-height: var(--max-height);
		}
		img.full {
			max-height: inherit;
		}

		.box {
			background: var(--bg1);
			box-shadow: 0 0 3px 4px #ccb inset;
			padding: 1em;
			padding-left: 1em;
		}
		.box .box {
			background: var(--bg2);
		}
		#output pre, .box table {
			padding-left: 2em;
		}

		th {
			text-align: left;
		}
		#columns {
			margin-left: 1.5em;
			border: 1px solid var(--border0);
		}
		#columns td, #columns th {
			padding: 2px 0.5em;
		}
		#columns td ~ td { /* Not leftmost one */
			border-left: 1px solid var(--border1);
		}
		#columns tr:nth-child(even) {
			background: var(--bg0dark);
		}

		.spinner {
			margin: 5px auto;
			width: 32px;
			height: 32px;
			border-radius: 50%;
			border: 8px solid;
			border-color: transparent red transparent green;
			animation: spinner 3s ease-in-out infinite;
		}
		@keyframes spinner {
			50% { transform: rotate(360deg); }
		}
	</style>
</head>
