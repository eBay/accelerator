<!DOCTYPE html>
<head>
	<title>ax board{{ title and ' - ' + title }}</title>
	<style>
		:root {
			--bg0: #fffef7;
			--bg1: #f9fff6;
			--bg2: #fff8f3;
			--not-loaded: #dcb;
			--bg0dark: #f8f4f1;
			--bg-click: #fbb;
			--fg0: #222;
			--border0: #ecece8;
			--border1: #ccb;
			--border2: #aad;
			--max-height: 300px;
		}
		a:link { color: #11F; }
		a:visited { color: #529; }
		a:active { color: #C16; }
		body {
			background: var(--bg0);
			color: var(--fg0);
		}
		input {
			background: var(--bg1);
			color: var(--fg0);
			border-color: var(--border1);
			border-radius: 5px;
			padding: 3px 1em;
		}
		input:focus {
			background: var(--bg2);
		}

		a[href="/"] {
			float: right;
		}
		.error {
			background: red;
		}

		#workdirs {
			float: right;
			margin-bottom: 1em;
		}
		#workdirs td {
			padding-right: 1em;
		}
		.textfile {
			max-height: var(--max-height);
			overflow: auto;
			background: var(--bg1);
			border: 2px solid var(--border1);
			padding-left: 1em;
		}
		.result {
			border-top: 2px solid var(--border2);
			padding-top: 10px;
			margin-bottom: 22px;
			clear: both;
		}
		.clickme {
			text-align: center;
			font-size: 120%;
			padding: 15px;
			background: var(--bg-click);
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
		form {
			display: inline;
		}

		.box {
			background: var(--bg1);
			box-shadow: 0 0 3px 4px var(--border1) inset;
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
		.ds-table {
			margin-left: 1.5em;
			border: 1px solid var(--border0);
		}
		.ds-table td, .ds-table th {
			padding: 2px 0.5em;
		}
		.ds-table td ~ td, .ds-table th ~ th { /* Not leftmost one */
			border-left: 1px solid var(--border1);
		}
		.ds-table tbody tr:nth-child(odd) {
			background: var(--bg0dark);
		}
		input[type="number"] {
			width: 8em;
		}
		td.not-loaded {
			background: var(--not-loaded);
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
		td .spinner {
			position: absolute;
			margin: 0;
		}
	</style>
</head>
