def print_dataframe(name, dataframe, truncate=True):
    print(name)
    print_shape(dataframe)
    dataframe.show(50, truncate)

def print_shape(dataframe):
    print("(" + str(dataframe.count()) + ", " + str(len(dataframe.columns)) + ")")