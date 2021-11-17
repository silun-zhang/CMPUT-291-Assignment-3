# A3Main.py
import matplotlib.pyplot as plt
from Q4A3 import *
from Q2A3 import *

# Generate graph 4
def graph_four():
    runtimes = q4_main()
    stacked_bar_chart(runtimes, 4)
    runtimes = q2_main()
    stacked_bar_chart(runtimes, 2)
    return

# Generates layered bar chart
def stacked_bar_chart(runtimes, query):
    print(runtimes)
    labels = ['SmallDB', 'MediumDB', 'LargeDB']
    small_runtimes = []
    medium_runtimes = []
    large_runtimes = []
    small_medium_runtimes = []
    small_runtimes.append(runtimes[0])
    small_runtimes.append(runtimes[3])
    small_runtimes.append(runtimes[6])
    medium_runtimes.append(runtimes[1])
    medium_runtimes.append(runtimes[4])
    medium_runtimes.append(runtimes[7])
    large_runtimes.append(runtimes[2])
    large_runtimes.append(runtimes[5])
    large_runtimes.append(runtimes[8])
    small_medium_runtimes.append((small_runtimes[0])+(medium_runtimes[0]))
    small_medium_runtimes.append(small_runtimes[1]+medium_runtimes[1])
    small_medium_runtimes.append(small_runtimes[2]+medium_runtimes[2])
    width = 0.35       # the width of the bars: can also be len(x) sequence
    
    fig, ax = plt.subplots()
    
    ax.bar(labels, small_runtimes, width, label='Uninformed')
    ax.bar(labels, medium_runtimes, width, bottom=small_runtimes, label='Self Optimized')
    ax.bar(labels, large_runtimes, width, bottom=small_medium_runtimes, label='User Optimized')

    ax.set_title('Optimized DB Query Runtimes')
    ax.legend()
    
    path = './Q' + str(query) + 'A3chart.png'
    plt.savefig(path)
    print('Chart saved to file Q4A3chart.png'.format(path))
    
    # close figure so it doesn't display
    plt.close() 
    return

def main():
    graph_four()
    return

if __name__ == "__main__":
    main()