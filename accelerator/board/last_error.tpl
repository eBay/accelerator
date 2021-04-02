{{ ! template('head', title='last error') }}

% from datetime import datetime
% from accelerator.error import JobError

% if not get('time'):
	<p>No error.</p>
% else:
	<p>Last error at {{ datetime.fromtimestamp(time).replace(microsecond=0) }}</p>
	% for jobid, method, status in last_error:
		<p><pre>{{ JobError(jobid, method, status).format_msg() }}</pre><p>
	% end
% end
</body>
