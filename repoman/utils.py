import sys                      # Mostly to flush stdout..
import time
from pathlib import Path
from functools import wraps
from typing import Callable


def get_user_history_path():
    history_path = Path("~/.config/repoman").expanduser()
    history_path.mkdir(parents=True, exist_ok=True)

    history_file = history_path / Path(".cli_history")
    if not history_file.exists():
        open(history_file, "a").close()
    return history_file


class AnonymousObj:
    def __init__(self, *args, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class progressIndicator:
    """Progress indicator for command line display use."""
    def __init__(self, level='medium', title='', intermediate_stats=0, noSymbols=False):
        self.intermediate_stats = intermediate_stats
        self.count        = 0
        self.time_started = time.time()
        self.time_taken   = 0
        self.noSymbols    = noSymbols
        self.symbolMinor  = '.'
        self.symbolMajor  = '+'
        if title:
            print(title)
        if level.lower() == 'mondohigh':
            self._major  = 50000
            self._minor  = 5000
            self._row    = 1000
        elif level.lower() == 'veryhigh':
            self._major  = 5000
            self._minor  = 500
            self._row    = 100
        elif level.lower() == 'high':
            self._major  = 500
            self._minor  = 50
            self._row    = 10
        elif level.lower() == 'medium':
            self._major  = 250
            self._minor  = 25
            self._row    = 5
        else:
            self._major  = 50
            self._minor  = 5
            self._row    = 1


    def update(self) -> None:
        self.count = self.count + 1
        if (self.count % self._major) == 0 and self.count > (self._major - 1):
            if not self.noSymbols:
                print(self.symbolMajor, end='')

            if not self.intermediate_stats:
                self._printStatistics(time.time())

        elif (self.count % self._minor) == 0 and self.count > (self._minor - 1):
            if not self.noSymbols:
                print(self.symbolMajor, end='')

        elif (self.count % self._row  ) == 0 and self.count > 0:
            if not self.noSymbols:
                print(self.symbolMinor, end='')
        sys.stdout.flush()


    def final(self, print_statistics=True) -> None:
        time_ = time.time()
        if print_statistics:
            self._printStatistics(time_)
        else:
            sys.stdout.write("\n") # Wrap up the display nicely.

        # Set the total time taken for anyone who wants it afterwards.
        self.time_taken = time_ - self.time_started
        sys.stdout.flush()


    def _printStatistics(self, arg_time) -> float:
        if self.count:
            spt = (arg_time - self.time_started) / self.count
            tps = spt
            if spt:
                tps = 1 / spt
                # Print either seconds per txn or txns per second depending
                # on whichever is larger..
                if tps > spt:
                    print(" (%7d @ %-8.2f tps)" % (self.count, tps))
                else:
                    print(" (%7d @ %-8.2f spt)" % (self.count, spt))
            else:
                print(" (%7d @ %8s spt)" % (self.count, '-----.--'))
            sys.stdout.flush()


class timer(object):
    def __init__(self, description):
        self.description = description

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, type, value, traceback):
        self.end = time.time()
        print(f"{self.description}: {self.end - self.start}")


def retry(ExceptionToCheck, tries=5, delay=1, backoff=2, logger=None) -> Callable:
    """Retry calling the decorated function using an exponential backoff.

    http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
    original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry

    :param ExceptionToCheck: the exception to check. may be a tuple of exceptions to check
    :type  ExceptionToCheck: Exception or tuple

    :param tries: number of times to try (not retry) before giving up
    :type  tries: int

    :param delay: initial delay between retries in seconds
    :type  delay: int

    :param backoff: backoff multiplier e.g. value of 2 will double the delay each retry
    :type  backoff: int

    :param logger: logger to use. If None, print
    :type  logger: logging.Logger instance
    """
    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck:
                    msg = f"{str(ExceptionToCheck)}, Retrying in {mdelay:.2f} seconds..."
                    if logger:
                        logger.warning(msg)
                    # else:
                    #     print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry


suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
def humanify_size(nbytes):
    i = 0
    while nbytes >= 1024 and i < len(suffixes)-1:
        nbytes /= 1024.
        i += 1
    str_ = ('%.2f' % nbytes).rstrip('0').rstrip('.')
    return f'{str_} {suffixes[i]}'
