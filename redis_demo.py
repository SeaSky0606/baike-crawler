import redis


def do_run():
    r = redis.Redis(host='bmredis01', port=6379, db=8)
    r.set("test.foo", "bar")
    for key in r.keys("test*"):
        print key, r.get(key)


if __name__ == '__main__':
    do_run()
    print "all done!"
