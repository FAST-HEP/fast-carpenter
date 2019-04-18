.. _ref-t2df_convert_to_long_form:

t2df_convert_to_long_form
=========================

The `t2df_convert_to_long_form <https://gitlab.cern.ch/fast-cms/FAST-RA1/blob/master/bin/t2df_convert_to_long_form>`_ executable runs the third step, which is contained in `convert_to_tai_form.py <https://gitlab.cern.ch/fast-cms/FAST-RA1/blob/master/fast_ra1/trees_to_dataframe/convert_to_tai_form.py>`_.

In this step, all the systematic variations are combined into a single data frame, which is the input for :ref:`ref-dataframe2datacards`.

There are two main ways of achieving this: adding a new yield column for each systematic variation (`wide` format), or adding a new categorical column with the systematic name and appending all the data frames (`long` format).

We choose to convert to long format because it tends to be more scalable as the set of column names remains fixed (and small), even with arbitrarily many systematic variations.

These operations are performed by the :file:`bin/t2df_convert_to_long_form` executable::

 python bin/t2df_convert_to_long_form output

In particular, the following lines of `convert_to_tai_form.py <https://gitlab.cern.ch/fast-cms/FAST-RA1/blob/master/fast_ra1/trees_to_dataframe/convert_to_tai_form.py#L52>`_ add the new column and append all the data frames::

 this_df["systematic"] = title
 if final_form is None:
     final_form = this_df
 else:
     final_form = final_form.append(this_df, ignore_index=True)


