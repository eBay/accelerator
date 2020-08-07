<!DOCTYPE html>
<head>
	<title>ax board{{ title and ' - ' + title }}</title>
	<style>
		:root {
			color-scheme: light dark;
			--bg0: #fffef7;
			--bg1: #f9fff6;
			--bg2: #fff8f3;
			--bgwarn: #fd7;
			--not-loaded: #dcb;
			--bg0dark: #f8f4f1;
			--bg-click: #fbb;
			--fg0: #222;
			--checkmark: #193;
			--border0: #ecece8;
			--border1: #ccb;
			--border2: #aad;
			--max-height: 300px;
		}
		a:link { color: #11F; }
		a:visited { color: #529; }
		a:active { color: #C16; }

		@media (prefers-color-scheme: dark) {
			:root {
				--bg0: #302a12;
				--bg1: #032;
				--bg2: #301;
				--bgwarn: #c71;
				--not-loaded: #654;
				--bg0dark: #113;
				--bg-click: #854;
				--fg0: #eec;
				--checkmark: #9eb;
				--border0: #552;
				--border1: #774;
				--border2: #549;
			}
			a:link { color: #6cf; }
			a:visited { color: #caf; }
			a:active { color: #f9b; }
		}

		body {
			background: var(--bg0);
			color: var(--fg0);
		}
		input {
			-webkit-appearance: none;
			appearance: none;
			background: var(--bg1);
			color: var(--fg0);
			border-color: var(--border1);
			border-radius: 5px;
			padding: 3px 1em;
		}
		input:focus {
			background: var(--bg2);
		}
		input[type="checkbox"] {
			position: relative;
			padding: 0;
			width: 1.2em;
			height: 1.2em;
			vertical-align: -0.22em;
			border: 1px solid var(--border2);
			color: var(--checkmark);
		}
		input[type="checkbox"]::before {
			content: "✔";
			position: absolute;
			top: -0.1em;
			left: 0.05em;
			font-size: 1.15em;
			visibility: hidden;
		}
		input[type="checkbox"]:checked::before {
			visibility: visible;
		}

		a[href="/"] {
			float: right;
		}
		.error {
			background: red;
		}
		.warning {
			background: var(--bgwarn);
			font-weight: bold;
			padding: 3px;
		}

		#header {
			float: left;
		}
		#waiting {
			position: fixed;
			left: 0;
			top: 5em;
			width: 100%;
			z-index: 1;
		}
		#status {
			background: var(--bg1);
			border: 2px solid var(--border1);
			padding: 2px 0.5em;
			margin-right: 1em;
		}
		#bonus-info {
			float: right;
			margin-bottom: 1em;
		}
		#methods {
			border-top: 2px solid var(--border1);
			display: table;
			margin: 0 1em 0 auto;
			padding: 3px 0 5px 0;
		}
		#workdirs {
			margin-left: auto;
			margin-right: 0;
			text-align: right;
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
		#other-params {
			margin-top: 1em;
		}
		#other-params table {
			padding-left: 0;
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

		.method-table {
			border-collapse: collapse;
			border-top: 1px solid var(--border1);
		}
		.method-table tbody td {
			vertical-align: top;
			padding: 3px 0.3em;
		}
		.method-table tbody td:last-child {
			width: 100%;
		}
		.method-table tbody tr:nth-child(odd) {
			background: var(--bg0dark);
		}
		.method-table tbody tr {
			border-bottom: 1px solid var(--border1);
		}

		#status-stacks td {
			padding: 1px 0.5em;
		}
		.output {
			background: var(--bg1);
			border: 2px solid var(--border1);
			margin-left: 4em;
			padding: 4px;
		}
		.output pre {
			font-family: monospace;
			margin-left: 2em;
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
