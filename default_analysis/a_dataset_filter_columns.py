description = r"""Make only some columns from a dataset visible"""

from extras import RequiredOption

options = dict(
	columns = RequiredOption(["colname1", "colname2", "..."]),
)

datasets = ("source",)

def synthesis():
	datasets.source.link_to_here(column_filter=options.columns)
