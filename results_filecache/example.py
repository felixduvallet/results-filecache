from results_filecache import results_cache
from functools import partial



# The first method of caching the result of a computation (big_task) just
# decorates the method directly. We used partial to select the filename and
# expected hash arguments.
filename = 'bigtask.pck'
expected_md5 = '89ae471d2783a5bdaca71cd91d2f3274'
cache_big_task = partial(results_cache.cached_call,
                         cache_filename=filename,
                         expected_hash=expected_md5)
@cache_big_task
def big_task(x):
    print('Running a big task: {}'.format(x))
    return sorted(x)


# In the second case, we will re-assign the function inside another function,
# via a helper method (see call_long_task inside run_example).
def long_task(a, b):
    print('Running a long task: {} {}'.format(a, b))
    return a + b


def run_example():
    x = [1, 3, 2]
    y = [2, 4, 7]

    def call_long_task(*args, **kwargs):
        return long_task(*args, **kwargs)

    # Method 2: re-assign function to the decorated version of itself. In a
    # class, just re-assign self.foo to cached_call(self.foo). In a module, you
    # need a local helper function (call_long_task in this case) that you *can*
    # access and wrap.
    filename = 'longtask.pck'
    expected_md5 = '60945c2971bf4c5f03fe54525902afe1'
    # Here we re-assign the function.
    call_long_task = results_cache.cached_call(
        call_long_task, filename, expected_md5)

    # NOTE: The first time, the file will not exist, so we will call the
    # computation.
    ret = call_long_task(x, y)
    print 'Result: ', ret

    # The second time, the file exists *and* its md5sum matches, so the result
    # is loaded from the longtask.pkl file instead.
    ret = call_long_task(x, y)
    print 'Result: ', ret

    # Here big_task was directly decorated. The first call will perform the
    # computation. The second call will use the file.
    ret = big_task(x)
    print 'Result: ', ret
    ret = big_task(x)
    print 'Result: ', ret

    import os
    try:
        os.remove(filename)
        os.remove('bigtask.pck')
    except OSError:
        pass


if __name__ == '__main__':
    run_example()
