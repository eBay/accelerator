<!DOCTYPE html>
<head>
	<title>ax board{{ title and ' - ' + title }}</title>
	<style>
		:root {
			color-scheme: light dark;
			--bg0: #fffef7;
			--bg1: #f5f8ef;
			--bg2: #f5f2e8;
			--bgwarn: #fd7;
			--bgerr: #b42;
			--fgerr: #fce;
			--not-loaded: #dcb;
			--bg0-odd: #f8f4f1;
			--bg-click: #fbb;
			--fg0: #222;
			--fg-a: #11f;
			--fg-a-v: #529;
			--fg-a-a: #c16;
			--fg-weak: #988890;
			--checkmark: #193;
			--border0: #e4e1d0;
			--border1: #e7e5d6;
			--border2: #aad;
			--max-height: 300px;
		}

		@media (prefers-color-scheme: dark) {
			:root {
				--bg0: #3a2b1a;
				--bg1: #3f3425;
				--bg2: #493724;
				--bgwarn: #a41;
				--not-loaded: #654;
				--bg0-odd: #4e3d2f;
				--bg-click: #944;
				--fg0: #ddc;
				--fg-a: #8cf;
				--fg-a-v: #caf;
				--fg-a-a: #f9b;
				--fg-weak: #877;
				--checkmark: #9eb;
				--border0: #764;
				--border1: #875;
				--border2: #549;
			}
		}

		body {
			background: var(--bg0);
			color: var(--fg0);
		}
		a:link { color: var(--fg-a); }
		a:visited { color: var(--fg-a-v); }
		a:active { color: var(--fg-a-a); }
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
		input[type="submit"] {
			margin-bottom: 1em;
		}
		input:disabled {
			opacity: 0.5;
		}

		a[href="/"] {
			float: right;
		}
		.error, input:focus.error {
			background: var(--bgerr);
			color: var(--fgerr);
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
			pointer-events: none;
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
			float: left;
			clear: left;
			min-width: 60%;
		}
		#bonus-info {
			float: right;
			margin-bottom: 1em;
		}
		#bonus-info ul {
			list-style-type: none;
			display: table;
			margin: 0 1em 0 auto;
			padding: 3px 0 5px 0;
		}
		#workdirs {
			margin-left: auto;
			margin-right: 0;
			text-align: right;
			border-bottom: 1px solid var(--border1);
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
			clear: both;
			overflow: hidden;
			animation: result 0.25s forwards linear;
		}
		@keyframes result {
			0% {
				transform: scale(0.5);
				opacity: 0.2;
				max-height: 1px;
			}
			99% {
				max-height: var(--max-height);
			}
			100% {
				transform: none;
				opacity: 1;
				max-height: none;
				overflow: visible;
				border-top: 2px solid var(--border2);
				padding-top: 10px;
				margin-bottom: 22px;
			}
		}
		.result > input[type="submit"] {
			float: right;
			margin-top: -3px;
			margin-bottom: 3px;
		}
		.result > div {
			clear: both;
		}
		.hidden {
			overflow: hidden;
			animation: hidden 0.3s forwards linear;
		}
		@keyframes hidden {
			0% {
				max-height: var(--max-height);
			}
			100% {
				transform: scale(0.5);
				opacity: 0.2;
				max-height: 1px;
				display: inline-table;
				visibility: collapse;
				border: 0;
				padding: 0;
				margin: 0;
			}
		}
		#show-all {
			float: right;
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
			margin: 0 0 0.5em 1.5em;
			border: 1px solid var(--border0);
		}
		.ds-table td, .ds-table th {
			padding: 2px 0.5em;
		}
		.ds-table td ~ td, .ds-table th ~ th { /* Not leftmost one */
			border-left: 1px solid var(--border1);
		}
		.ds-table tbody tr:nth-child(odd) {
			background: var(--bg0-odd);
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
			background: var(--bg0-odd);
		}
		.method-table tbody tr {
			border-bottom: 1px solid var(--border1);
		}

		.urd-table {
			border: 1px solid var(--border0);
		}
		.urd-table tbody td {
			vertical-align: top;
			padding: 0.3em 0.5em;
			border-bottom: 1px solid var(--border1);
		}
		.urd-table tbody tr:last-child td {
			border-bottom: 0;
		}
		.urd-table ol {
			list-style-position: inside;
			padding: 0;
			margin: 0.3em 0 0 2em;
		}
		.urd-table ol:first-child {
			margin: 0;
		}
		.urd-table li::marker {
			color: var(--fg-weak);
		}

		table.job-table {
			border-spacing: 0 2px;
			clear: both;
		}
		table.job-table td:nth-child(2) {
			padding: 0 1em;
			min-width: 13em;
		}
		table.job-table td:last-child {
			color: var(--fg-weak);
		}
		table.job-table tr.filtered {
			visibility: collapse;
		}
		table.job-table tr.error {
			visibility: visible;
		}
		.filter {
			position: fixed;
			top: 0;
			right: 0;
			border-left: 1px solid var(--border2);
			border-bottom: 1px solid var(--border2);
			padding: 1em;
		}
		@media (max-width: 60em) {
			.workdir > h1 {
				float: left;
			}
			.filter {
				position: relative;
				float: right;
			}
		}
		.filter h1 {
			margin: 0 0 0.5em 0;
			font-size: 130%;
			font-weight: lighter;
		}
		.filter input[type="text"] {
			width: 11em;
		}
		.filter tbody td {
			padding-right: 0.3em;
		}
		table.job-table tr.unfinished {
			background: var(--bgwarn);
		}
		table.job-table tr.old {
			background: var(--bg2);
		}

		#status-stacks td {
			padding: 1px 1px;
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
			pointer-events: none;
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
