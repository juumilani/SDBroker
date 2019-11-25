from collector import Collector
from threading import Thread
from random import uniform

def main():
    for i in range(20):
        print(f'Connecting collector #{i}...')
        c = Collector(str(i), uniform(0.5, 3.0))
        Thread(target=c.run).start()
       
if __name__ == '__main__':
    main()
