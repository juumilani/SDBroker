from reader import Reader
from threading import Thread
from random import randint

def main():
    for i in range(20):
        print(f'Connecting reader #{i}...')
        r = Reader()
        r.register(str(randint(0,19)))
        Thread(target=r.consume).start()
       
if __name__ == '__main__':
    main()
