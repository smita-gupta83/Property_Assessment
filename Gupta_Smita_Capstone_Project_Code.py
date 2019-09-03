"""
Boston Property Assessment Python Script

Copyright (c) 2019
Licensed
Written by Smita Gupta
"""


def data_manipulation(data, column_name, manipulation_type, manipulation_value):

    """
    This function has been created for data manipulation

    Parameters
    -----------
    data: corresponds to dataframe on which manipulation needs to be done
    column_name: corresponds to name of the column on which manipulation operation need to be done
    manipulation_type: corresponds to type of manipulation such as-:
                fill missing by median - when imputation of missing value needs to be done based on median value of
                                         the column passed in the function
                fill missing by mean - when imputation of missing values needs to be done based on mean value of
                                         the column passed in the function
                fill missing by 0 - when imputation of missing values by zero is needed
                calc_aggregation - when aggregate value needs to be created such as mean, median, max and so on
                remove end special char - when a special charcter needs to be removed from end of the column
                rename column values- when column values need to be replaced with other values
                convert data type -  when the data type of the column need to be changes like int to float
                check uniqueness -  when uniqueness of a column need be verified
    manipulation_value: corresponds to value with which manipulation needs to be carried out

    Returns
    ----------
    This function will return the manipulated column as per manipulation type

    """

    # defined common missing value imputation types in a list
    operation_types = ['fill missing by median', 'fill missing by mean', 'fill missing by 0', 'calc aggregation']
    if manipulation_type in operation_types:
        if manipulation_type == 'calc aggregation':
            column_updated = data.agg({column_name: manipulation_value})
        elif manipulation_type == 'fill missing by median':
            column_median = data[column_name].median()
            column_updated = data[column_name].fillna(column_median)
        elif manipulation_type == 'fill missing by mean':
            column_mean = data[column_name].mean()
            column_updated = data[column_name].fillna(column_mean)
        elif manipulation_type == 'fill missing by 0':
            column_zero = 0
            column_updated = data[column_name].fillna(column_zero)
    elif manipulation_type == "remove end special char":
        find_char_position = data[column_name].str.find(manipulation_value).unique()
        column_updated = data[column_name].str.slice(0, find_char_position[0])
    elif manipulation_type == "rename column values":
        column_updated = data[column_name].replace(manipulation_value)
    elif manipulation_type == "convert data type":
        column_updated = data[column_name].astype(manipulation_value)
    elif manipulation_type == "check uniqueness":
        column_updated = data[column_name].is_unique
        if column_updated:
            print("The dataset has no duplicate property row based on {}.".format(column_name))
        else:
            print("The dataset duplicate property row based on {}.".format(column_name))
    return column_updated


def column_group_aggregation(data, column_to_group, column_to_aggr, aggregation_type):

    """
    This function has been created to calculate aggregation based on grouping of a particular column

    Parameters
    -----------
    data: corresponds to the dataframe on which aggregation needs to be performed
    column_to_group: corresponds to column name which needs to be grouped
    column_to_aggr: corresponds to column name that needs to be aggregated
    aggregation_type: corresponds to type of aggregation function such mean, median,sum

    Returns
    ----------
    This function will return the column with aggregated value

    """

    column_group = data.groupby(column_to_group, as_index=False)
    column_aggr = column_group.agg({column_to_aggr: aggregation_type})
    return column_aggr


def change_multi_to_single_value(data, column_name, from_value, to_value):

    """
    This function has been created to replace multiple values with a single value in a column

    Parameters
    -----------
    data: corresponds to the dataframe
    column_name: corresponds to column name which contains the multiple values
    from_value: list of multiple column values that needs to be replaced
    to_value: corresponds to replacement  value

    Returns
    ----------
    This function will return the column with replaced value

    """

    rename_column_value = data[column_name].replace(from_value, to_value)
    return rename_column_value


def find_annotation_position(data, row_indexer, column_indexer, x_pos):

    """
    This function has been created to annotate a datapoint on a plot

    Parameters
    -----------
    data: corresponds to the dataframe
    row_indexer: corresponds to row index ( or x-axis column)
    column_indexer: corresponds to column index (or y-axis column)
    x_pos: corresponds to position of x-coordinate

    Returns
    ----------
    This function will return the annotated position of y-axis

    """
    annotate_ypos_mean = column_group_aggregation(data, row_indexer, column_indexer, "max")
    annotate_position = annotate_ypos_mean.loc[annotate_ypos_mean[row_indexer] == x_pos, [column_indexer]]
    y_coordinate = annotate_position[column_indexer].item()
    return y_coordinate


def calc_prevalence(data, column_name):

    """
    This function has been created for calculating prevalence of the positive class

    Parameter
    ----------
    data: corresponds to data frame
    column_name : corresponds to the label column for which prevalence need to be determined

    Return
    ---------

    This function returns the prevalence of positive class with label = 1

    """

    prevalence_value = data[column_name].values
    prevalence = sum(prevalence_value)/len(prevalence_value)
    return prevalence







