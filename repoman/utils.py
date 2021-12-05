import sys                      # Mostly to flush stdout..
import time
from pathlib import Path

def get_user_history_path():
    history_path = Path("~/.config/repoman/.cli_history").expanduser()
    if not history_path.exists() or not history_path.is_file():
        open(history_path, "a").close()
    return history_path


class AnonymousObj:
    def __init__(self, *args, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class progressIndicator:
    """Progress indicator for command line display use."""
    def __init__(self, level='medium', title='', noIntermediateStats=0, noSymbols=False):
        self.__noIntermediateStats = noIntermediateStats
        self.__count       = 0
        self.__timeStarted = time.time()
        self.noSymbols     = noSymbols
        self.symbolMinor   = '.'
        self.symbolMajor   = '+'
        if title:
            print(title)
        if level.lower() == 'mondohigh':
            self._major        = 50000
            self._minor        = 5000
            self._row          = 1000
        elif level.lower() == 'veryhigh':
            self._major        = 5000
            self._minor        = 500
            self._row          = 100
        elif level.lower() == 'high':
            self._major        = 500
            self._minor        = 50
            self._row          = 10
        elif level.lower() == 'medium':
            self._major        = 250
            self._minor        = 25
            self._row          = 5
        else:
            self._major        = 50
            self._minor        = 5
            self._row          = 1

    def _printStatistics(self):
        if self.__count:
            spt = (time.time() - self.__timeStarted) / self.__count
            tps = spt
            if spt:
                tps = 1 / spt
                if tps > spt: print(" (%7d @ %-8.2f tps)" % (self.__count,tps))
                else:         print(" (%7d @ %-8.2f spt)" % (self.__count,spt))
            else:
                print(" (%7d @ %8s spt)" % (self.__count, '-----.--'))

    def update(self):
        if   (self.__count % self._major) == 0 and self.__count > (self._major - 1):
            if not self.noSymbols:
                print(self.symbolMajor, end='')
            if not self.__noIntermediateStats:
                self._printStatistics()

        elif (self.__count % self._minor) == 0 and self.__count > (self._minor - 1):
            if not self.noSymbols:
                print(self.symbolMajor, end='')

        elif (self.__count % self._row  ) == 0 and self.__count > 0:
            if not self.noSymbols:
                print(self.symbolMinor, end='')
        self.__count = self.__count + 1
        sys.stdout.flush()

    def getCount(self):
        return self.__count

    def final(self):
        print()
        self._printStatistics()
        sys.stdout.flush()

pi = progressIndicator
