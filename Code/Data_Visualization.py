import matplotlib.pyplot as plt
import pandas as pd

def Graph_for_query3():
    data = pd.read_csv("/Users/satyaprakash/PycharmProjects/Python_Spark/Data/AAPL.csv")
    data_100 = data.head(100)
    plt.figure(facecolor='#D5D8DC')
    plt.axes().set_facecolor("#EBEDEF")
    plt.plot(data_100['Date'], data_100['Open'], label="Open")
    plt.plot(data_100['Date'], data_100['High'], label="High")
    plt.plot(data_100['Date'], data_100['Low'], label="Low")
    plt.legend()
    plt.title("AAPL High Low and Open with Dates")
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.xticks(fontsize=7, rotation=75)
    plt.show()

Graph_for_query3()