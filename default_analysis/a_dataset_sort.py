# Stable sort a dataset based on one or more columns.
# You'll have to type the sort column approprietly.

from __future__ import division

from numpy import lexsort
from os import symlink

from gzwrite import typed_writer
from extras import job_params, mk_splitdir, OptionEnum, OptionString
from jobid import resolve_jobid_filename
from chaining import jobchain, iterate_datasets
from dataset import dataset
import blob

OrderEnum = OptionEnum('ascending descending')

options = {
	'sort_columns'           : [OptionString],
	'sort_order'             : OrderEnum.ascending,
	'sort_across_slices'     : False, # normally only sort within slices
}
datasets = ('source', 'previous',)


def sort(columniter, columns):
	def sortable_columnlist(column):
		if columns[column] in ('datetime', 'date', 'time',):
			return map(str, columniter(column))
		else:
			return list(columniter(column))
	lst = [sortable_columnlist(c) for c in reversed(options.sort_columns)]
	if options.sort_order == 'descending':
		# Stupid lexsort doesn't take a direction, and we want stable sorting.
		[e.reverse() for e in lst]
		l = len(lst[0]) - 1
		sort_idx = [l - i for i in reversed(lexsort(lst))]
	else:
		sort_idx = list(lexsort(lst)) # stable
	return sort_idx

def prepare():
	prev_p = job_params(datasets.previous, default_empty=True)
	d = dataset()
	d.load(datasets.source)
	columns = d.name_type_dict()
	jobs = jobchain(tip_jobid=datasets.source, stop_jobid=prev_p.datasets.source)
	if options.sort_across_slices:
		def columniter(column):
			return iterate_datasets(None, column, jobs)
		sort_idx = sort(columniter, columns)
	else:
		sort_idx = None
	return columns, jobs, sort_idx

def analysis(sliceno, params, prepare_res):
	columns, jobs, sort_idx = prepare_res
	single_job = (len(jobs) == 1)
	if options.sort_across_slices:
		def columniter(column):
			return iterate_datasets(None, column, jobs)
		per_slice = len(sort_idx) // params.slices
		if sliceno + 1 ==  params.slices:
			sort_idx = sort_idx[per_slice * sliceno:]
		else:
			sort_idx = sort_idx[per_slice * sliceno:per_slice * (sliceno + 1)]
	else:
		def columniter(column):
			return iterate_datasets(sliceno, column, jobs)
		sort_idx = sort(columniter, columns)
	if single_job and sort_idx == sorted(sort_idx) and not options.sort_across_slices:
		# this slice is fully sorted as is.
		slice_dir = '%02d' % (sliceno,)
		symlink(resolve_jobid_filename(datasets.source, slice_dir), slice_dir)
		return len(sort_idx)
	dstdir = mk_splitdir(sliceno)
	def writer(column):
		W = typed_writer(columns[column])
		return W('%s/%s.gz' % (dstdir, column,))
	for column in columns:
		lst = list(columniter(column))
		with writer(column) as w:
			for idx in sort_idx:
				w.write(lst[idx])
	return len(sort_idx)

def synthesis(params, analysis_res):
	d = dataset()
	d.load(datasets.source)
	if options.sort_across_slices:
		hashlabel = None
	else:
		hashlabel = d.get_hashlabel()
	dataset().new(params.jobid,
		name_type=d.name_type_dict(),
		caption=params.caption,
		num_lines_per_split=list(analysis_res),
		hashlabel=hashlabel,
		filename=d.get_filename(),
	)
	# Preserve minmax.pickle if source has it.
	try:
		minmax = blob.load('minmax', datasets.source)
		blob.save(minmax, 'minmax')
	except IOError:
		pass
